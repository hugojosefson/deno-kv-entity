import { assertEquals as eq } from "https://deno.land/std@0.188.0/testing/asserts.ts";
import {
  beforeAll,
  beforeEach,
  describe,
  it,
} from "https://deno.land/std@0.188.0/testing/bdd.ts";
import { EntityDb } from "../src/entity-db.ts";
import { asArray, Maybe } from "../src/fn.ts";
import { EntityDefinition, EntityInstance } from "../src/types.ts";

export const TEST_PREFIX: string[] = [import.meta.url];

const ENTITY_DEFINITION_PERSON: EntityDefinition<Person> = {
  id: "person",
  uniqueProperties: ["ssn", "email"] as Array<keyof Person>,
  indexedPropertyChains: [
    ["lastname", "firstname"],
    ["country", "zipcode"],
  ] as Array<keyof Person>[],
  _exampleEntityInstance: {} as Person,
} as EntityDefinition<Person>;

const ENTITY_DEFINITION_INVOICE: EntityDefinition<Invoice> = {
  id: "invoice",
  uniqueProperties: ["invoiceNumber"] as Array<keyof Invoice>,
  indexedPropertyChains: [
    ["customerEmail"],
  ] as Array<keyof Invoice>[],
  _exampleEntityInstance: {} as Invoice,
} as EntityDefinition<Invoice>;

interface Person extends EntityInstance<Record<string, string>> {
  ssn: string;
  email: string;
  firstname: string;
  lastname: string;
  country: string;
  zipcode: string;
}

interface Invoice extends EntityInstance<Record<string, string>> {
  invoiceNumber: string;
  customerEmail: string;
}

const ALICE: Person = {
  ssn: "123-45-6789",
  email: "alice@example.com",
  firstname: "Alice",
  lastname: "Smith",
  country: "US",
  zipcode: "12345",
} as const;

const BOB: Person = {
  ssn: "987-65-4321",
  email: "bob@example.com",
  firstname: "Bob",
  lastname: "Jones",
  country: "US",
  zipcode: "12345",
} as const;

let db: EntityDb<Person | Invoice>;

beforeAll(() => {
  db = new EntityDb<Person | Invoice>({
    prefix: TEST_PREFIX,
    entityDefinitions: {
      person: ENTITY_DEFINITION_PERSON as EntityDefinition<Person>,
      invoice: ENTITY_DEFINITION_INVOICE as EntityDefinition<Invoice>,
    },
  });
});

beforeEach(async () => {
  await db.clearAllEntities();
});

describe("db", () => {
  describe("save", () => {
    it("should save a Person", async () => {
      const person: Person = ALICE;
      await db.save("person", person);
      const actual: Maybe<Person> = await db.find(
        "person",
        "ssn",
        ALICE.ssn,
      );
      eq(actual, person);
    });

    it("should save an Invoice", async () => {
      const invoice: Invoice = {
        invoiceNumber: "123",
        customerEmail: ALICE.email,
      };
      await db.save("invoice", invoice);
      const actual: Maybe<Invoice> = await db.find(
        "invoice",
        "invoiceNumber",
        "123",
      );
      eq(actual, invoice);
    });
  });
  describe("findAll", () => {
    it("should find all Persons", async () => {
      await db.save("person", ALICE);
      await db.save("person", BOB);

      const actualPerson1: Maybe<Person> = await db.find(
        "person",
        "ssn",
        ALICE.ssn,
      );
      eq(actualPerson1, ALICE);

      const actualPerson2: Maybe<Person> = await db.find(
        "person",
        "ssn",
        BOB.ssn,
      );
      eq(actualPerson2, BOB);

      const actualPersons: Person[] = await db.findAll(
        "person",
        [
          ["country", "US"],
          ["zipcode", "12345"],
        ],
      );
      eq(actualPersons, [ALICE, BOB]);
    });

    it("should find only all Alice's Invoices", async () => {
      const invoice1: Invoice = {
        invoiceNumber: "123",
        customerEmail: ALICE.email,
      };
      const invoice2: Invoice = {
        invoiceNumber: "456",
        customerEmail: ALICE.email,
      };
      const invoice3: Invoice = {
        invoiceNumber: "789",
        customerEmail: BOB.email,
      };
      await db.save("invoice", invoice1);
      await db.save("invoice", invoice2);
      await db.save("invoice", invoice3);
      const actual: Invoice[] = await db.findAll("invoice", [[
        "customerEmail",
        ALICE.email,
      ]]);
      eq(actual, [invoice1, invoice2]);
    });
  });
  describe("empty db by default", () => {
    it("should return undefined for a Person", async () => {
      await assertFind<Person>(db, undefined, ["person", "ssn", ALICE.ssn]);
    });
    it("should return undefined for an Invoice", async () => {
      await assertFind<Invoice>(db, undefined, [
        "invoice",
        "invoiceNumber",
        "123",
      ]);
    });
    it('should return empty array for findAll("person, country, zipcode)', async () => {
      await assertFindAll<Person>(
        db,
        [],
        ["person", [
          ["country", "US"],
          ["zipcode", "12345"],
        ]],
      );
    });
    it('should return empty array for findAll("person", customerEmail)', async () => {
      await assertFindAll<Invoice>(
        db,
        [],
        ["invoice", [
          ["customerEmail", ALICE.email],
        ]],
      );
    });
    it('should return empty array for findAll("person")', async () => {
      await assertFindAll<Person>(db, [], ["person"]);
    });
    it('should return empty array for findAll("invoice")', async () => {
      await assertFindAll<Invoice>(db, [], ["invoice"]);
    });
    it("should return empty array for findAll()", async () => {
      await assertFindAll<Person | Invoice>(db, [], []);
    });
  });
  describe("clearEntity", () => {
    it("should clear all Persons, leave Invoices", async () => {
      const invoice1: Invoice = {
        invoiceNumber: "123",
        customerEmail: ALICE.email,
      };
      const invoice2: Invoice = {
        invoiceNumber: "456",
        customerEmail: BOB.email,
      };
      await db.save("person", ALICE);
      await db.save("person", BOB);
      await db.save("invoice", invoice1);
      await db.save("invoice", invoice2);

      await assertFindAll<Person>(db, [ALICE, BOB], ["person", "ssn"]);
      await assertFindAll<Invoice>(db, [invoice1, invoice2], [
        "invoice",
        "invoiceNumber",
      ]);

      await db.clearEntity("person");
      await assertFindAll<Person>(db, [], ["person", "ssn"]);
      await assertFindAll<Invoice>(db, [invoice1, invoice2], [
        "invoice",
        "invoiceNumber",
      ]);
    });
  });
  describe("delete", () => {
    it("should delete a Person via ssn", async () => {
      await db.save("person", ALICE);
      await assertFind(db, ALICE, ["person", "ssn", ALICE.ssn]);

      await db.delete("person", "ssn", ALICE.ssn);
      await assertFind(db, undefined, ["person", "ssn", ALICE.ssn]);

      // check that the person was deleted from other indices
      await assertFind(db, undefined, ["person", "email", ALICE.email]);
      await assertFindAll(db, [], [
        "person",
        [
          ["country", "US"],
          ["zipcode", "12345"],
        ],
      ]);
      await assertFindAll(db, [], ["person"]);
    });
    it("should delete an Invoice via invoiceNumber", async () => {
      const invoice: Invoice = {
        invoiceNumber: "123",
        customerEmail: ALICE.email,
      };
      await db.save("invoice", invoice);
      await assertFind(db, invoice, ["invoice", "invoiceNumber", "123"]);

      await db.delete("invoice", "invoiceNumber", "123");
      await assertFind(db, undefined, ["invoice", "invoiceNumber", "123"]);
    });
  });
});

/**
 * Assert that an EntityInstance exists in the EntityDb.
 *
 * @param db The EntityDb to check.
 * @param expected The expected EntityInstance.
 * @param findArgs The arguments to pass to db.find.
 */
async function assertFind<T extends EntityInstance<T>>(
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
async function assertFindAll<T extends EntityInstance<T>>(
  db: EntityDb<T>,
  expected: T[],
  findAllArgs: Parameters<EntityDb<T>["findAll"]>,
): Promise<void> {
  const actual: T[] = await db.findAll(...findAllArgs);
  eq(actual, expected);
}

/**
 * Assert that the EntityDb contains the given key-value pairs.
 * @param db The EntityDb to check.
 * @param expected The expected key-value pairs.
 * @returns
 * @throws AssertionError if the EntityDb does not contain the expected key-value pairs.
 * @throws AssertionError if the EntityDb contains additional key-value pairs.
 * @throws AssertionError if the EntityDb contains the expected key-value pairs, but the values are not equal.
 */
async function assertDbIs<T extends [Deno.KvKey, unknown][]>(
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

describe("Entire DB", () => {
  it("should be empty by default", async () => {
    await assertDbIs(db, []);
  });
  it("should contain a Person", async () => {
    await db.save("person", ALICE);
    await assertDbIs(db, [
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_PERSON.id,
          ...["country", ALICE.country],
          ...["zipcode", ALICE.zipcode],
          ALICE.ssn,
        ],
        ALICE,
      ],
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_PERSON.id,
          ...["email", ALICE.email],
        ],
        ALICE,
      ],
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_PERSON.id,
          ...["lastname", ALICE.lastname],
          ...["firstname", ALICE.firstname],
          ALICE.ssn,
        ],
        ALICE,
      ],
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_PERSON.id,
          ...["ssn", ALICE.ssn],
        ],
        ALICE,
      ],
    ]);
  });
  it("should contain an Invoice", async () => {
    const invoice: Invoice = {
      invoiceNumber: "123",
      customerEmail: ALICE.email,
    };
    await db.save("invoice", invoice);
    await assertDbIs(db, [
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_INVOICE.id,
          ...["customerEmail", invoice.customerEmail],
          invoice.invoiceNumber,
        ],
        invoice,
      ],
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_INVOICE.id,
          ...["invoiceNumber", invoice.invoiceNumber],
        ],
        invoice,
      ],
    ]);
  });
  it("should contain a Person and an Invoice", async () => {
    await db.save("person", ALICE);
    const invoice: Invoice = {
      invoiceNumber: "123",
      customerEmail: ALICE.email,
    };
    await db.save("invoice", invoice);
    await assertDbIs(db, [
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_PERSON.id,
          ...["country", ALICE.country],
          ...["zipcode", ALICE.zipcode],
          ALICE.ssn,
        ],
        ALICE,
      ],
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_PERSON.id,
          ...["email", ALICE.email],
        ],
        ALICE,
      ],
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_PERSON.id,
          ...["lastname", ALICE.lastname],
          ...["firstname", ALICE.firstname],
          ALICE.ssn,
        ],
        ALICE,
      ],
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_PERSON.id,
          ...["ssn", ALICE.ssn],
        ],
        ALICE,
      ],
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_INVOICE.id,
          ...["customerEmail", invoice.customerEmail],
          invoice.invoiceNumber,
        ],
        invoice,
      ],
      [
        [
          import.meta.url,
          ENTITY_DEFINITION_INVOICE.id,
          ...["invoiceNumber", invoice.invoiceNumber],
        ],
        invoice,
      ],
    ]);
  });
});
