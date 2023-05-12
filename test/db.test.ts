import { assertStrictEquals as eq } from "https://deno.land/std@0.187.0/testing/asserts.ts";
import {
  beforeEach,
  describe,
  it,
} from "https://deno.land/std@0.187.0/testing/bdd.ts";
import { doWithDb } from "../src/db.ts";

const TEST_PREFIX = import.meta.url;

beforeEach(() =>
  doWithDb(async (db: Deno.Kv) => {
    const iterator = db.list({ prefix: [TEST_PREFIX] });
    for await (const { key } of iterator) {
      await db.delete(key);
    }
  })
);

describe("db", () => {
  it("should work", async () => {
    const actual = await doWithDb(async (db: Deno.Kv) => {
      await db.set([TEST_PREFIX, "foo"], "bar");
      return (await db.get([TEST_PREFIX, "foo"])).value;
    });
    eq(actual, "bar");
  });
  it("should handle atomic transactions", async () => {
    const actual = await doWithDb(async (db: Deno.Kv) => {
      await db.atomic()
        .set([TEST_PREFIX, "foo"], "bar")
        .set([TEST_PREFIX, "baz"], "qux")
        .commit();
      return (await db.get([TEST_PREFIX, "foo"])).value;
    });
    eq(actual, "bar");
  });
});
