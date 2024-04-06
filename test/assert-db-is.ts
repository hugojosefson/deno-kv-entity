import { EntityDb } from "../src/entity-db.ts";
import { asArray } from "../src/fn.ts";
import { assertEquals as eq } from "https://deno.land/std@0.221.0/testing/asserts.ts";

/**
 * Assert that the EntityDb contains the given key-value pairs.
 * @param db The EntityDb to check.
 * @param expected The expected key-value pairs.
 * @returns
 * @throws AssertionError if the EntityDb does not contain the expected key-value pairs.
 * @throws AssertionError if the EntityDb contains additional key-value pairs.
 * @throws AssertionError if the EntityDb contains the expected key-value pairs, but the values are not equal.
 */
export async function assertDbIs<T extends [Deno.KvKey, unknown][]>(
  // deno-lint-ignore no-explicit-any
  db: EntityDb<any>,
  expected: T,
): Promise<void> {
  const actual: [Deno.KvKey, unknown][] = await db._doWithConnection(
    expected,
    async (conn) => {
      const entriesIterator = conn.list({ prefix: [] });
      const entries: Deno.KvEntry<unknown>[] = await asArray(entriesIterator);
      return entries.map((entry) => [entry.key, entry.value]);
    },
  );
  eq(actual.toSorted(), expected.toSorted());
}
