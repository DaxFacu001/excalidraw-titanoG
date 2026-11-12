import { syncInvalidIndices } from "@excalidraw/element";
import {
  encryptData,
  generateEncryptionKey,
} from "@excalidraw/excalidraw/data/encryption";
import { API } from "@excalidraw/excalidraw/tests/helpers/api";
import { vi } from "vitest";

import type { ExcalidrawElement, FileId } from "@excalidraw/element/types";
import type { AppState } from "@excalidraw/excalidraw/types";

import { getSyncableElements } from "../data";

import type { Socket } from "socket.io-client";

import type Portal from "../collab/Portal";

// In-memory stand-in for Supabase that mirrors the semantics of
// get_scene / save_scene in supabase-schema.sql (verified against Postgres).
const fake = vi.hoisted(() => {
  import.meta.env.VITE_APP_SUPABASE_URL = "https://test.supabase.co";
  import.meta.env.VITE_APP_SUPABASE_ANON_KEY = "anon-key";

  type Row = {
    scene_version: number;
    iv: string;
    ciphertext: string;
    rev: number;
  };
  const state = {
    rows: new Map<string, Row>(),
    objects: new Map<string, Uint8Array>(),
    saveCalls: 0,
    // runs right before the next save_scene is evaluated, to simulate another
    // client writing between our read and our write
    beforeSave: null as null | (() => Promise<void> | void),
  };

  const client = {
    rpc: async (name: string, args: any) => {
      if (name === "get_scene") {
        const row = state.rows.get(args.p_id);
        return { data: row ? [row] : [], error: null };
      }
      if (name === "save_scene") {
        state.saveCalls++;
        if (state.beforeSave) {
          const hook = state.beforeSave;
          state.beforeSave = null;
          await hook();
        }
        const row = state.rows.get(args.p_id);
        if (args.p_expected_rev === null) {
          if (row) {
            return { data: null, error: null };
          }
          state.rows.set(args.p_id, {
            scene_version: args.p_scene_version,
            iv: args.p_iv,
            ciphertext: args.p_ciphertext,
            rev: 1,
          });
          return { data: 1, error: null };
        }
        if (!row || row.rev !== args.p_expected_rev) {
          return { data: null, error: null };
        }
        row.scene_version = args.p_scene_version;
        row.iv = args.p_iv;
        row.ciphertext = args.p_ciphertext;
        row.rev += 1;
        return { data: row.rev, error: null };
      }
      throw new Error(`unexpected rpc ${name}`);
    },
    storage: {
      from: () => ({
        upload: async (path: string, body: Uint8Array) => {
          if (state.objects.has(path)) {
            return {
              data: null,
              error: {
                statusCode: "409",
                message: "The resource already exists",
              },
            };
          }
          state.objects.set(path, body);
          return { data: { path }, error: null };
        },
      }),
    },
  };

  return { state, client };
});

vi.mock("../data/supabaseSDK", () => ({
  createClient: () => fake.client,
}));

// imported after the mocks/env are in place
const {
  saveToFirebase,
  loadFromFirebase,
  saveFilesToFirebase,
  isSavedToFirebase,
} = await import("../data/firebase");

const appState = {} as AppState;

const scene = (elements: ExcalidrawElement[]) =>
  getSyncableElements(syncInvalidIndices(elements));

const makePortal = (roomId: string, roomKey: string) =>
  ({ roomId, roomKey, socket: {} as Socket } as unknown as Portal);

const ids = (elements: readonly { id: string }[] | null) =>
  (elements ?? []).map((el) => el.id).sort();

describe("Supabase persistence layer", () => {
  beforeEach(() => {
    fake.state.rows.clear();
    fake.state.objects.clear();
    fake.state.saveCalls = 0;
    fake.state.beforeSave = null;
  });

  it("saves a new scene and loads it back (encrypted round trip)", async () => {
    const roomId = "a".repeat(20);
    const roomKey = await generateEncryptionKey();
    const portal = makePortal(roomId, roomKey);
    const elements = scene([API.createElement({ type: "rectangle", id: "A" })]);

    const stored = await saveToFirebase(portal, elements, appState);

    expect(ids(stored)).toEqual(["A"]);
    expect(fake.state.rows.get(roomId)?.rev).toBe(1);
    // nothing readable is stored
    expect(fake.state.rows.get(roomId)?.ciphertext).not.toContain("rectangle");
    expect(isSavedToFirebase(portal, elements)).toBe(true);

    const loaded = await loadFromFirebase(roomId, roomKey, null);
    expect(ids(loaded)).toEqual(["A"]);
  });

  it("returns null when the room does not exist", async () => {
    const key = await generateEncryptionKey();
    expect(await loadFromFirebase("b".repeat(20), key, null)).toBeNull();
  });

  it("cannot decrypt with a different key", async () => {
    const roomId = "c".repeat(20);
    const portal = makePortal(roomId, await generateEncryptionKey());
    await saveToFirebase(
      portal,
      scene([API.createElement({ type: "rectangle", id: "A" })]),
      appState,
    );

    await expect(
      loadFromFirebase(roomId, await generateEncryptionKey(), null),
    ).rejects.toThrow();
  });

  it("retries and reconciles when another client saved in between", async () => {
    const roomId = "d".repeat(20);
    const roomKey = await generateEncryptionKey();
    const portal = makePortal(roomId, roomKey);

    // first save creates the row (rev 1) with element A
    await saveToFirebase(
      portal,
      scene([API.createElement({ type: "rectangle", id: "A" })]),
      appState,
    );

    // we now add element B locally, but right before our write lands another
    // client stores element C (rev 1 -> 2)
    const other = scene([
      API.createElement({ type: "rectangle", id: "A" }),
      API.createElement({ type: "rectangle", id: "C" }),
    ]);
    fake.state.beforeSave = async () => {
      const { encryptedBuffer, iv } = await encryptData(
        roomKey,
        new TextEncoder().encode(JSON.stringify(other)),
      );
      const row = fake.state.rows.get(roomId)!;
      row.iv = btoa(String.fromCharCode(...iv));
      row.ciphertext = btoa(
        String.fromCharCode(...new Uint8Array(encryptedBuffer)),
      );
      row.rev += 1;
    };

    fake.state.saveCalls = 0;
    const stored = await saveToFirebase(
      portal,
      scene([
        API.createElement({ type: "rectangle", id: "A" }),
        API.createElement({ type: "rectangle", id: "B" }),
      ]),
      appState,
    );

    // first attempt lost the race (null), second one succeeded
    expect(fake.state.saveCalls).toBe(2);
    expect(fake.state.rows.get(roomId)?.rev).toBe(3);
    // nothing got lost: A, B (ours) and C (theirs)
    expect(ids(stored)).toEqual(["A", "B", "C"]);
    expect(ids(await loadFromFirebase(roomId, roomKey, null))).toEqual([
      "A",
      "B",
      "C",
    ]);
  });

  it("gives up with an error after too many conflicts", async () => {
    const roomId = "e".repeat(20);
    const roomKey = await generateEncryptionKey();
    const portal = makePortal(roomId, roomKey);
    await saveToFirebase(
      portal,
      scene([API.createElement({ type: "rectangle", id: "A" })]),
      appState,
    );

    // every attempt loses the race
    const original = fake.client.rpc;
    fake.client.rpc = async (name: string, args: any) => {
      if (name === "save_scene") {
        fake.state.rows.get(roomId)!.rev += 1;
      }
      return original(name, args);
    };

    try {
      await expect(
        saveToFirebase(
          portal,
          // two elements so the scene version differs from the one already
          // saved above (otherwise the save is skipped as "already saved")
          scene([
            API.createElement({ type: "rectangle", id: "B" }),
            API.createElement({ type: "rectangle", id: "D" }),
          ]),
          appState,
        ),
      ).rejects.toThrow(/too many concurrent updates/);
    } finally {
      fake.client.rpc = original;
    }
  });

  it("treats an already uploaded file as saved and normalizes the path", async () => {
    const files = [
      { id: "file1" as FileId, buffer: new Uint8Array([1, 2, 3]) },
    ];

    const first = await saveFilesToFirebase({
      prefix: "/files/rooms/room1",
      files,
    });
    expect(first.savedFiles).toEqual(["file1"]);
    expect([...fake.state.objects.keys()]).toEqual(["files/rooms/room1/file1"]);

    // same content hash uploaded again -> 409 from Storage -> still "saved"
    const second = await saveFilesToFirebase({
      prefix: "files/rooms/room1",
      files,
    });
    expect(second.savedFiles).toEqual(["file1"]);
    expect(second.erroredFiles).toEqual([]);
  });
});
