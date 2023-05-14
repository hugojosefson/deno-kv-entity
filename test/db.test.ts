import { assertStrictEquals as eq } from "https://deno.land/std@0.187.0/testing/asserts.ts";
import {
  beforeEach,
  describe,
  it,
} from "https://deno.land/std@0.187.0/testing/bdd.ts";
import { DataObject, Db, Entity } from "../src/db.ts";

const TEST_PREFIX: string[] = [import.meta.url];

const ENTITY_PERSON: Entity<Person> = {
  id: "person",
  uniqueProperties: ["ssn", "email"],
  nonUniqueLookupPropertyChains: [
    ["lastname", "firstname"],
    ["country", "zipcode"],
  ],
} as Entity<Person>;

const ENTITY_INVOICE: Entity<Invoice> = {
  id: "invoice",
  uniqueProperties: ["invoiceNumber"],
  nonUniqueLookupPropertyChains: [
    ["customerEmail"],
  ],
} as Entity<Invoice>;

const MY_ENTITIES: {
  person: Entity<Person>;
  invoice: Entity<Invoice>;
} = {
  person: ENTITY_PERSON as Entity<Person>,
  invoice: ENTITY_INVOICE as Entity<Invoice>,
} as {
  person: Entity<Person>;
  invoice: Entity<Invoice>;
};

class Person implements DataObject<Person> {
  readonly _entityId = "person";
  constructor(
    public readonly ssn: string,
    public readonly email: string,
    public readonly firstname: string,
    public readonly lastname: string,
    public readonly country: string,
    public readonly zipcode: string,
  ) {}
}

class Invoice implements DataObject<Invoice> {
  readonly _entityId = "invoice";
  constructor(
    public readonly invoiceNumber: string,
    public readonly customerEmail: string,
  ) {}
}

let db: Db<
  Person | Invoice
>;

beforeEach(() => {
  db = new Db<
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
