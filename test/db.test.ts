import { assertEquals as eq } from "https://deno.land/std@0.187.0/testing/asserts.ts";
import {
  beforeAll,
  beforeEach,
  describe,
  it,
} from "https://deno.land/std@0.187.0/testing/bdd.ts";
import { Db, Entity } from "../src/db.ts";

export const TEST_PREFIX: string[] = [import.meta.url];

const ENTITY_PERSON: Entity<Person> = {
  id: "person",
  uniqueProperties: ["ssn", "email"] as Array<keyof Person>,
  nonUniqueLookupPropertyChains: [
    ["lastname", "firstname"],
    ["country", "zipcode"],
  ] as Array<keyof Person>[],
  _exampleInstance: {} as Person,
} as Entity<Person>;

const ENTITY_INVOICE: Entity<Invoice> = {
  id: "invoice",
  uniqueProperties: ["invoiceNumber"] as Array<keyof Invoice>,
  nonUniqueLookupPropertyChains: [
    ["customerEmail"],
  ] as Array<keyof Invoice>[],
  _exampleInstance: {} as Invoice,
} as Entity<Invoice>;

interface Person {
  ssn: string;
  email: string;
  firstname: string;
  lastname: string;
  country: string;
  zipcode: string;
}

interface Invoice {
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

let db: Db<Person | Invoice>;

beforeAll(() => {
  db = new Db<Person | Invoice>({
    prefix: TEST_PREFIX,
    entities: {
      person: ENTITY_PERSON as Entity<Person>,
      invoice: ENTITY_INVOICE as Entity<Invoice>,
    },
  });
});

beforeEach(async () => {
  await db.clearAllEntities();
});

describe("db", () => {
  describe("save", () => {
    it("should put a Person", async () => {
      const person: Person = ALICE;
      await db.save("person", person);
      const actual: Person | undefined = await db.find(
        "person",
        "ssn",
        ALICE.ssn,
      );
      eq(actual, person);
    });

    it("should put an Invoice", async () => {
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
      const actual: unknown[] = await db.findAll<
        Person | Invoice,
        | "ssn"
        | "email"
        | "firstname"
        | "lastname"
        | "country"
        | "zipcode"
        | "invoiceNumber"
        | "customerEmail"
      >();
      eq(actual, []);
    });
  });
});
