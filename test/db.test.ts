import { assertStrictEquals as eq } from "https://deno.land/std@0.187.0/testing/asserts.ts";
import {
  beforeEach,
  describe,
  it,
} from "https://deno.land/std@0.187.0/testing/bdd.ts";
import { DataObject, Db, Entity } from "../src/db.ts";

const TEST_PREFIX: string[] = [import.meta.url];

const ENTITY_PERSON: Entity<"person", Person> = {
  id: "person",
  uniqueProperties: ["ssn", "email"],
  nonUniqueLookupPropertyChains: [
    ["lastname", "firstname"],
    ["country", "zipcode"],
  ],
} as Entity<"person", Person>;

const ENTITY_INVOICE: Entity<"invoice", Invoice> = {
  id: "invoice",
  uniqueProperties: ["invoiceNumber"],
  nonUniqueLookupPropertyChains: [
    ["customerEmail"],
  ],
} as Entity<"invoice", Invoice>;

const MY_ENTITIES: {
  person: Entity<"person", Person>;
  invoice: Entity<"invoice", Invoice>;
} = {
  person: ENTITY_PERSON,
  invoice: ENTITY_INVOICE,
} as {
  person: Entity<"person", Person>;
  invoice: Entity<"invoice", Invoice>;
};

class Person implements DataObject<Person> {
  readonly _dataTypeId = "person";
  constructor(
    public ssn: string,
    public email: string,
    public firstname: string,
    public lastname: string,
    public country: string,
    public zipcode: string,
  ) {}
}

class Invoice implements DataObject<Invoice> {
  readonly _dataTypeId = "invoice";
  constructor(
    public invoiceNumber: string,
    public customerEmail: string,
  ) {}
}

let db: Db<
  "person" | "invoice",
  Person | Invoice
>;

beforeEach(() => {
  db = new Db<
    "person" | "invoice",
    Person | Invoice
  >({
    prefix: TEST_PREFIX,
    entities: MY_ENTITIES,
  });
});

describe("db", () => {
  describe("save", () => {
    it("should put a Person", async () => {
      const person = new Person(
        "123-45-6789",
        "alice@example.com",
        "Alice",
        "Smith",
        "US",
        "12345",
      );
      await db.save(ENTITY_PERSON, person);
      const actual = await db.find(
        ENTITY_PERSON,
        "ssn",
        "123-45-6789",
      );
      eq(actual, person);
    });

    it("should put an Invoice", async () => {
      const invoice = new Invoice("123", "alice@example.com");
      await db.save(ENTITY_INVOICE, invoice);
      const actual = await db.find(
        ENTITY_INVOICE,
        "invoiceNumber",
        "123",
      );
      eq(actual, invoice);
    });
  });
});
