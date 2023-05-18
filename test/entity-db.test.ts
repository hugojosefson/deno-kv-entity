import { assertEquals as eq } from "https://deno.land/std@0.188.0/testing/asserts.ts";
import {
  beforeAll,
  beforeEach,
  describe,
  it,
} from "https://deno.land/std@0.188.0/testing/bdd.ts";
import {
  EntityDb,
  EntityDefinition,
  EntityInstance,
} from "../src/entity-db.ts";

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
      const actual: Person | undefined = await db.find(
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
      const actual: Invoice | undefined = await db.find(
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

      const actualPerson1: Person | undefined = await db.find(
        "person",
        "ssn",
        ALICE.ssn,
      );
      eq(actualPerson1, ALICE);

      const actualPerson2: Person | undefined = await db.find(
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
      const actual: Person | undefined = await db.find(
        "person",
        "ssn",
        ALICE.ssn,
      );
      eq(actual, undefined);
    });
    it("should return undefined for an Invoice", async () => {
      const actual: Invoice | undefined = await db.find(
        "invoice",
        "invoiceNumber",
        "123",
      );
      eq(actual, undefined);
    });
    it('should return empty array for findAll("person, country, zipcode)', async () => {
      const actual: Person[] = await db.findAll(
        "person",
        [
          ["country", "US"],
          ["zipcode", "12345"],
        ],
      );
      eq(actual, []);
    });
    it('should return empty array for findAll("person", customerEmail)', async () => {
      const actual: Invoice[] = await db.findAll("invoice", [[
        "customerEmail",
        ALICE.email,
      ]]);
      eq(actual, []);
    });
    it('should return empty array for findAll("person")', async () => {
      const actual: Person[] = await db.findAll("person");
      eq(actual, []);
    });
    it('should return empty array for findAll("invoice")', async () => {
      const actual: Invoice[] = await db.findAll("invoice");
      eq(actual, []);
    });
    it("should return empty array for findAll()", async () => {
      const actual: unknown[] = await db.findAll<Person | Invoice>();
      eq(actual, []);
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

      const actualPersons = await db.findAll("person", "ssn");
      eq(actualPersons, [ALICE, BOB]);

      const actualInvoices = await db.findAll("invoice", "invoiceNumber");
      eq(actualInvoices, [invoice1, invoice2]);

      await db.clearEntity("person");

      const actualPersons2 = await db.findAll("person", "ssn");
      eq(actualPersons2, []);

      const actualInvoices2 = await db.findAll("invoice", "invoiceNumber");
      eq(actualInvoices2, [invoice1, invoice2]);
    });
  });
});
