/**
 * Await an async iterable iterator, and return all the results as an array.
 * @param iterator the iterator to await
 * @returns the results as an array
 */
export async function awaitAsyncIterableIterator<T>(
  iterator: AsyncIterableIterator<T>,
): Promise<T[]> {
  const results: T[] = [];
  for await (const result of iterator) {
    results.push(result);
  }
  return results;
}

export function prop<T>(name: keyof T): (obj: T) => T[keyof T] {
  return (obj: T) => obj[name];
}
