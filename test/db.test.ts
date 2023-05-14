import { assertStrictEquals as eq } from "https://deno.land/std@0.187.0/testing/asserts.ts";
import {
  beforeEach,
  describe,
  it,
} from "https://deno.land/std@0.187.0/testing/bdd.ts";
import { Db, Entity } from "../src/db.ts";

const TEST_PREFIX: string[] = [import.meta.url];

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
});
