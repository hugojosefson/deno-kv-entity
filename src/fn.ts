/**
 * Await an async iterable iterator, and return all the results as an array.
 * @param iterator the iterator to await
 * @returns the results as an array
 */
export async function asArray<T>(
  iterator: AsyncIterableIterator<T>,
): Promise<T[]> {
  const results: T[] = [];
  for await (const result of iterator) {
    results.push(result);
  }
  return results;
}

/**
 * Creates a function that gets a property's value from an object.
 * @param name the name of the property to get
 */
export function prop<T>(name: keyof T): (obj: T) => T[keyof T] {
  return (obj: T) => obj[name];
}

/** An instance of something of the type void. */
export const VOID: void = undefined as void;

/** Whether a value is a Deno.KvKeyPart */
export function isKvKeyPart(value: unknown): value is Deno.KvKeyPart {
  return [
    "string",
    "number",
    "bigint",
    "boolean",
  ].includes(typeof value) ||
    value?.constructor === Uint8Array;
}

/** Whether a value is a Deno.KvKey */
export function isKvKey(value: unknown): value is Deno.KvKey {
  return Array.isArray(value) && value.every(isKvKeyPart);
}

/** Whether a value is not undefined */
export function isDefined<T>(value: T | undefined): value is T {
  return typeof value !== "undefined";
}

/** T or undefined */
export type Maybe<T> = T | undefined;
