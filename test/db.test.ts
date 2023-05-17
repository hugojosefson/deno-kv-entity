import { assertEquals as eq } from "https://deno.land/std@0.187.0/testing/asserts.ts";
import {
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

let db: Db<Person | Invoice>;

beforeEach(() => {
  db = new Db<Person | Invoice>({
    prefix: TEST_PREFIX,
    entities: {
      person: ENTITY_PERSON as Entity<Person>,
      invoice: ENTITY_INVOICE as Entity<Invoice>,
    },
  });
});

describe("db", () => {
  describe("save", () => {
    it("should put a Person", async () => {
      const person: Person = {
        ssn: "123-45-6789",
        email: "alice@example.com",
        firstname: "Alice",
        lastname: "Smith",
        country: "US",
        zipcode: "12345",
      };
      await db.save("person", person);
      const actual: Person | undefined = await db.find(
        "person",
        "ssn",
        "123-45-6789",
      );
      eq(actual, person);
    });

    it("should put an Invoice", async () => {
      const invoice: Invoice = {
        invoiceNumber: "123",
        customerEmail: "alice@example.com",
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
      const person1: Person = {
        ssn: "123-45-6789",
        email: "alice@example.com",
        firstname: "Alice",
        lastname: "Smith",
        country: "US",
        zipcode: "12345",
      };
      const person2: Person = {
        ssn: "987-65-4321",
        email: "bob@example.com",
        firstname: "Bob",
        lastname: "Jones",
        country: "US",
        zipcode: "12345",
      };
      await db.save("person", person1);
      await db.save("person", person2);

      const actualPerson1: Person | undefined = await db.find(
        "person",
        "ssn",
        "123-45-6789",
      );
      eq(actualPerson1, person1);

      const actualPerson2: Person | undefined = await db.find(
        "person",
        "ssn",
        "987-65-4321",
      );
      eq(actualPerson2, person2);

      const actualPersons: Person[] = await db.findAll(
        "person",
        [
          ["country", "US"],
          ["zipcode", "12345"],
        ],
      );
      eq(actualPersons, [person1, person2]);
    });

    it("should find only all Alice's Invoices", async () => {
      const invoice1: Invoice = {
        invoiceNumber: "123",
        customerEmail: "alice@example.com",
      };
      const invoice2: Invoice = {
        invoiceNumber: "456",
        customerEmail: "alice@example.com",
      };
      const invoice3: Invoice = {
        invoiceNumber: "789",
        customerEmail: "bob@example.com",
      };
      await db.save("invoice", invoice1);
      await db.save("invoice", invoice2);
      await db.save("invoice", invoice3);
      const actual: Invoice[] = await db.findAll("invoice", [[
        "customerEmail",
        "alice@example.com",
      ]]);
      eq(actual, [invoice1, invoice2]);
    });
  });
});
