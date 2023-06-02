import { EntityDefinition, EntityInstance } from "../src/types.ts";

export const ENTITY_DEFINITION_PERSON: EntityDefinition<Person> = {
  id: "person",
  uniqueProperties: ["ssn", "email"] as Array<keyof Person>,
  indexedPropertyChains: [
    ["lastname", "firstname"],
    ["country", "zipcode"],
  ] as Array<keyof Person>[],
  _exampleEntityInstance: {} as Person,
} as EntityDefinition<Person>;
export const ENTITY_DEFINITION_INVOICE: EntityDefinition<Invoice> = {
  id: "invoice",
  uniqueProperties: ["invoiceNumber"] as Array<keyof Invoice>,
  indexedPropertyChains: [
    ["customerEmail"],
  ] as Array<keyof Invoice>[],
  _exampleEntityInstance: {} as Invoice,
} as EntityDefinition<Invoice>;

export interface Person extends EntityInstance<Record<string, string>> {
  ssn: string;
  email: string;
  firstname: string;
  lastname: string;
  country: string;
  zipcode: string;
}

export interface Invoice extends EntityInstance<Record<string, string>> {
  invoiceNumber: string;
  customerEmail: string;
}

export const ALICE: Person = {
  ssn: "123-45-6789",
  email: "alice@example.com",
  firstname: "Alice",
  lastname: "Smith",
  country: "US",
  zipcode: "12345",
} as const;
export const BOB: Person = {
  ssn: "987-65-4321",
  email: "bob@example.com",
  firstname: "Bob",
  lastname: "Jones",
  country: "US",
  zipcode: "12345",
} as const;
