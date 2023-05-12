import {
  Deferred,
  deferred,
} from "https://deno.land/std@0.187.0/async/deferred.ts";

const instanceDeferred: Deferred<Deno.Kv> = deferred<Deno.Kv>();

export async function getInstance(): Promise<Deno.Kv> {
  if (instanceDeferred.state === "pending") {
    const instance = await Deno.openKv();
    instanceDeferred.resolve(instance);
  }
  return instanceDeferred;
}
