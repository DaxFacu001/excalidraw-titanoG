// Supabase SDK subset used by the app. Loaded lazily from `./firebase.ts`
// via `import("./supabaseSDK")` so it is not part of the main bundle.
//
// Static re-exports (rather than destructuring `await import(...)`) keep the
// SDK tree-shakeable.
export { createClient } from "@supabase/supabase-js";
