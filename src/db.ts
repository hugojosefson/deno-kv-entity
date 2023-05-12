export type DbCallback<T> = (db: Deno.Kv) => Promise<T> | T;

export async function doWithDb<T>(fn: DbCallback<T>): Promise<T> {
  const db: Deno.Kv = await Deno.openKv();
  try {
    return await fn(db);
  } finally {
    db.close();
  }
}

export async function doWithSpecificDb<T>(
  path: string,
  fn: DbCallback<T>,
): Promise<T> {
  const db: Deno.Kv = await Deno.openKv(path);
  try {
    return await fn(db);
  } finally {
    db.close();
  }
}

export function getDoWithDbFunctionForSpecificDb(
  path: string,
): typeof doWithDb {
  return async function <T>(fn: DbCallback<T>): Promise<T> {
    return await doWithSpecificDb(path, fn);
  };
}
