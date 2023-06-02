import { EntityInstance } from "../src/types.ts";
import { EntityDb } from "../src/entity-db.ts";
import { Maybe } from "../src/fn.ts";
import { assertEquals as eq } from "https://deno.land/std@0.188.0/testing/asserts.ts";

/**
 * Assert that an EntityInstance exists in the EntityDb.
 *
 * @param db The EntityDb to check.
 * @param expected The expected EntityInstance.
 * @param findArgs The arguments to pass to db.find.
 */
export async function assertFind<T extends EntityInstance<T>>(
  db: EntityDb<T>,
  expected: Maybe<T>,
  findArgs: Parameters<EntityDb<T>["find"]>,
): Promise<void> {
  const actual: Maybe<T> = await db.find(...findArgs);
  eq(actual, expected);
}

/**
 * Assert that an array of EntityInstances exists in the EntityDb.
 *
 * @param db The EntityDb to check.
 * @param expected The expected EntityInstances.
 * @param findAllArgs The arguments to pass to db.findAll.
 */
export async function assertFindAll<T extends EntityInstance<T>>(
  db: EntityDb<T>,
  expected: T[],
  findAllArgs: Parameters<EntityDb<T>["findAll"]>,
): Promise<void> {
  const actual: T[] = await db.findAll(...findAllArgs);
  eq(actual, expected);
}
