// Supabase-backed implementation of the persistence layer that upstream
// Excalidraw implements with Firebase (Firestore + Cloud Storage).
//
// The file name and the exported function names are intentionally unchanged so
// that no other module (Collab, App, ExportToExcalidrawPlus, data/index) has to
// be touched. That keeps this patch small and easy to re-apply on top of newer
// upstream versions.
//
// Storage model:
// - Collab scenes: table `scenes`, only reachable through the RPC functions
//   `get_scene` / `save_scene` (see supabase-schema.sql). Concurrency is handled
//   with optimistic locking on a `rev` counter instead of a Firestore
//   transaction.
// - Files (images): public Supabase Storage bucket, upload-only for anon.
//
// Everything stored is encrypted client-side with the room key, which lives in
// the URL fragment and never reaches Supabase.

import { reconcileElements } from "@excalidraw/excalidraw";
import { MIME_TYPES, toBrandedType } from "@excalidraw/common";
import { decompressData } from "@excalidraw/excalidraw/data/encode";
import {
  encryptData,
  decryptData,
} from "@excalidraw/excalidraw/data/encryption";
import { restoreElements } from "@excalidraw/excalidraw/data/restore";
import { getSceneVersion } from "@excalidraw/element";

import type { RemoteExcalidrawElement } from "@excalidraw/excalidraw/data/reconcile";
import type {
  ExcalidrawElement,
  FileId,
  OrderedExcalidrawElement,
} from "@excalidraw/element/types";
import type {
  AppState,
  BinaryFileData,
  BinaryFileMetadata,
  DataURL,
} from "@excalidraw/excalidraw/types";

import { FILE_CACHE_MAX_AGE_SEC } from "../app_constants";

import { getSyncableElements } from ".";

import type { SyncableExcalidrawElement } from ".";
import type Portal from "../collab/Portal";
import type { Socket } from "socket.io-client";

// private
// -----------------------------------------------------------------------------

const SUPABASE_URL = (import.meta.env.VITE_APP_SUPABASE_URL || "").replace(
  /\/+$/,
  "",
);
const SUPABASE_ANON_KEY = import.meta.env.VITE_APP_SUPABASE_ANON_KEY || "";
const SUPABASE_BUCKET =
  import.meta.env.VITE_APP_SUPABASE_BUCKET || "excalidraw-files";

// how many times to re-read, re-reconcile and retry when another client saved
// the same scene between our read and our write
const MAX_SAVE_ATTEMPTS = 5;

const lazy = <T>(load: () => Promise<T>) => {
  let promise: Promise<T> | null = null;
  return () =>
    (promise ??= load().catch((error) => {
      promise = null;
      throw error;
    }));
};

const _loadSupabase = lazy(async () => {
  // import first so that a failure to load the SDK chunk is what surfaces
  const { createClient } = await import("./supabaseSDK");

  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    throw new Error(
      "Supabase is not configured. Set VITE_APP_SUPABASE_URL and VITE_APP_SUPABASE_ANON_KEY.",
    );
  }

  return createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
    // no user accounts: the anon key only ever calls the RPC functions and
    // uploads files
    auth: {
      persistSession: false,
      autoRefreshToken: false,
      detectSessionInUrl: false,
    },
  });
});

const normalizePath = (path: string) => path.replace(/^\/+/, "");

const isAlreadyExistsError = (error: any) =>
  String(error?.statusCode) === "409" ||
  /already exists|duplicate/i.test(String(error?.message ?? ""));

const bytesToBase64 = (bytes: Uint8Array): string => {
  // chunked to stay clear of the argument limit of String.fromCharCode
  const CHUNK = 0x8000;
  let binary = "";
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return btoa(binary);
};

const base64ToBytes = (base64: string): Uint8Array<ArrayBuffer> => {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
};

// -----------------------------------------------------------------------------

type UploadMetadata = {
  customMetadata?: Record<string, string>;
};

export const uploadBytesToFirebase = async (
  path: string,
  data: Blob | Uint8Array,
  metadata?: UploadMetadata,
) => {
  const supabase = await _loadSupabase();
  const { error } = await supabase.storage
    .from(SUPABASE_BUCKET)
    .upload(normalizePath(path), data, {
      cacheControl: `${FILE_CACHE_MAX_AGE_SEC}`,
      contentType: MIME_TYPES.binary,
      upsert: false,
      metadata: metadata?.customMetadata,
    });
  if (error && !isAlreadyExistsError(error)) {
    throw error;
  }
};

type StoredScene = {
  iv: string; // base64
  ciphertext: string; // base64
};

type StoredSceneRow = StoredScene & {
  sceneVersion: number;
  rev: number;
};

const encryptElements = async (
  key: string,
  elements: readonly ExcalidrawElement[],
): Promise<StoredScene> => {
  const json = JSON.stringify(elements);
  const encoded = new TextEncoder().encode(json);
  const { encryptedBuffer, iv } = await encryptData(key, encoded);

  return {
    iv: bytesToBase64(iv),
    ciphertext: bytesToBase64(new Uint8Array(encryptedBuffer)),
  };
};

const decryptElements = async (
  data: StoredScene,
  roomKey: string,
): Promise<readonly ExcalidrawElement[]> => {
  const ciphertext = base64ToBytes(data.ciphertext);
  const iv = base64ToBytes(data.iv);

  const decrypted = await decryptData(iv, ciphertext, roomKey);
  const decodedData = new TextDecoder("utf-8").decode(
    new Uint8Array(decrypted),
  );
  return JSON.parse(decodedData);
};

const fetchStoredScene = async (
  roomId: string,
): Promise<StoredSceneRow | null> => {
  const supabase = await _loadSupabase();
  const { data, error } = await supabase.rpc("get_scene", { p_id: roomId });
  if (error) {
    throw error;
  }
  const row = Array.isArray(data) ? data[0] : data;
  if (!row) {
    return null;
  }
  return {
    sceneVersion: row.scene_version,
    iv: row.iv,
    ciphertext: row.ciphertext,
    rev: row.rev,
  };
};

class FirebaseSceneVersionCache {
  private static cache = new WeakMap<Socket, number>();
  static get = (socket: Socket) => {
    return FirebaseSceneVersionCache.cache.get(socket);
  };
  static set = (
    socket: Socket,
    elements: readonly SyncableExcalidrawElement[],
  ) => {
    FirebaseSceneVersionCache.cache.set(socket, getSceneVersion(elements));
  };
}

export const isSavedToFirebase = (
  portal: Portal,
  elements: readonly ExcalidrawElement[],
): boolean => {
  if (portal.socket && portal.roomId && portal.roomKey) {
    const sceneVersion = getSceneVersion(elements);

    return FirebaseSceneVersionCache.get(portal.socket) === sceneVersion;
  }
  // if no room exists, consider the room saved so that we don't unnecessarily
  // prevent unload (there's nothing we could do at that point anyway)
  return true;
};

export const saveFilesToFirebase = async ({
  prefix,
  files,
}: {
  prefix: string;
  files: { id: FileId; buffer: Uint8Array }[];
}) => {
  // failing to load the SDK must fail the whole save rather than be reported
  // per file as `erroredFiles` (which share links ignore, and collab never
  // retries)
  const supabase = await _loadSupabase();
  const bucket = supabase.storage.from(SUPABASE_BUCKET);

  const erroredFiles: FileId[] = [];
  const savedFiles: FileId[] = [];

  await Promise.all(
    files.map(async ({ id, buffer }) => {
      try {
        const { error } = await bucket.upload(
          `${normalizePath(prefix)}/${id}`,
          buffer,
          {
            cacheControl: `${FILE_CACHE_MAX_AGE_SEC}`,
            contentType: MIME_TYPES.binary,
            upsert: false,
          },
        );
        // file ids are content hashes, so "already exists" means the exact
        // same file is already stored: treat it as saved
        if (error && !isAlreadyExistsError(error)) {
          throw error;
        }
        savedFiles.push(id);
      } catch (error: any) {
        erroredFiles.push(id);
      }
    }),
  );

  return { savedFiles, erroredFiles };
};

export const saveToFirebase = async (
  portal: Portal,
  elements: readonly SyncableExcalidrawElement[],
  appState: AppState,
) => {
  const { roomId, roomKey, socket } = portal;
  if (
    // bail if no room exists as there's nothing we can do at this point
    !roomId ||
    !roomKey ||
    !socket ||
    isSavedToFirebase(portal, elements)
  ) {
    return null;
  }

  const supabase = await _loadSupabase();

  let storedScene: StoredScene | null = null;

  for (
    let attempt = 0;
    attempt < MAX_SAVE_ATTEMPTS && !storedScene;
    attempt++
  ) {
    const prevRow = await fetchStoredScene(roomId);

    let elementsToStore: readonly SyncableExcalidrawElement[] = elements;

    if (prevRow) {
      const prevStoredElements = getSyncableElements(
        restoreElements(await decryptElements(prevRow, roomKey), null),
      );
      elementsToStore = getSyncableElements(
        reconcileElements(
          elements,
          prevStoredElements as OrderedExcalidrawElement[] as RemoteExcalidrawElement[],
          appState,
        ),
      );
    }

    const candidate = await encryptElements(roomKey, elementsToStore);

    const { data: newRev, error } = await supabase.rpc("save_scene", {
      p_id: roomId,
      p_scene_version: getSceneVersion(elementsToStore),
      p_iv: candidate.iv,
      p_ciphertext: candidate.ciphertext,
      // null = create; otherwise only update if nobody saved in the meantime
      p_expected_rev: prevRow ? prevRow.rev : null,
    });

    if (error) {
      throw error;
    }

    // null means we lost the race against another client: loop again so we
    // reconcile against what they stored
    if (newRev !== null && newRev !== undefined) {
      storedScene = candidate;
    }
  }

  if (!storedScene) {
    throw new Error(
      `Could not save scene after ${MAX_SAVE_ATTEMPTS} attempts (too many concurrent updates)`,
    );
  }

  // Return the stored elements as the in memory elements could have mutated in
  // the meantime
  const storedElements = getSyncableElements(
    restoreElements(await decryptElements(storedScene, roomKey), null),
  );

  FirebaseSceneVersionCache.set(socket, storedElements);

  return toBrandedType<RemoteExcalidrawElement[]>(storedElements);
};

export const loadFromFirebase = async (
  roomId: string,
  roomKey: string,
  socket: Socket | null,
): Promise<readonly SyncableExcalidrawElement[] | null> => {
  const row = await fetchStoredScene(roomId);
  if (!row) {
    return null;
  }
  const elements = getSyncableElements(
    restoreElements(await decryptElements(row, roomKey), null, {
      deleteInvisibleElements: true,
    }),
  );

  if (socket) {
    FirebaseSceneVersionCache.set(socket, elements);
  }

  return elements;
};

export const loadFilesFromFirebase = async (
  prefix: string,
  decryptionKey: string,
  filesIds: readonly FileId[],
) => {
  const loadedFiles: BinaryFileData[] = [];
  const erroredFiles = new Map<FileId, true>();

  await Promise.all(
    [...new Set(filesIds)].map(async (id) => {
      try {
        const objectPath = `${normalizePath(prefix)}/${id}`
          .split("/")
          .map(encodeURIComponent)
          .join("/");
        // public bucket: no SDK (and no API key) needed to download
        const url = `${SUPABASE_URL}/storage/v1/object/public/${SUPABASE_BUCKET}/${objectPath}`;
        const response = await fetch(url);
        if (response.status < 400) {
          const arrayBuffer = await response.arrayBuffer();

          const { data, metadata } = await decompressData<BinaryFileMetadata>(
            new Uint8Array(arrayBuffer),
            {
              decryptionKey,
            },
          );

          const dataURL = new TextDecoder().decode(data) as DataURL;

          loadedFiles.push({
            mimeType: metadata.mimeType || MIME_TYPES.binary,
            id,
            dataURL,
            created: metadata?.created || Date.now(),
            lastRetrieved: metadata?.created || Date.now(),
          });
        } else {
          erroredFiles.set(id, true);
        }
      } catch (error: any) {
        erroredFiles.set(id, true);
        console.error(error);
      }
    }),
  );

  return { loadedFiles, erroredFiles };
};
