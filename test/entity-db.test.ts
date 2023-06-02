import { assertEquals as eq } from "https://deno.land/std@0.190.0/testing/asserts.ts";
import {
  beforeAll,
  beforeEach,
  describe,
  it,
} from "https://deno.land/std@0.190.0/testing/bdd.ts";
import { EntityDb } from "../src/entity-db.ts";
import { Maybe } from "../src/fn.ts";
import { EntityDefinition } from "../src/types.ts";
import {
  ALICE,
  BOB,
  ENTITY_DEFINITION_INVOICE,
  ENTITY_DEFINITION_PERSON,
  Invoice,
  Person,
} from "./fixtures.ts";
import { assertDbIs } from "./assert-db-is.ts";
import { assertFind, assertFindAll } from "./assert-find.ts";

export const TEST_PREFIX: string[] = [import.meta.url];

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
