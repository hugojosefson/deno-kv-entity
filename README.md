# kv_entity

Typed library for specifying and storing entities in a
[Deno.Kv](https://deno.com/kv) database.

[![deno module](https://shield.deno.dev/x/kv_entity)](https://deno.land/x/kv_entity)
[![CI](https://github.com/hugojosefson/deno-kv-entity/actions/workflows/ci.yaml/badge.svg)](https://github.com/hugojosefson/deno-kv-entity/actions/workflows/ci.yaml)

## Requirements

Requires [Deno](https://deno.land/) v1.42.1 or later, with the `--unstable-kv`
flag.

## API

Please see the
[auto-generated API documentation](https://deno.land/x/kv_entity?doc).

## Example usage

```typescript
import {
  EntityDb,
  EntityDefinition,
} from "https://deno.land/x/kv_entity/mod.ts";

// What your data looks like. These are yours. You define them,
// but each must have at least one unique property.
interface Person {
  email: string;
  name: string;
}
interface Invoice {
  invoiceNumber: string;
  customerEmail: string;
  amount: number;
}

// Describe them to the EntityDb
const personDefinition: EntityDefinition<Person> = {
  id: "person",
  uniqueProperties: ["email"],
  indexedPropertyChains: [],
  _exampleEntityInstance: {} as Person,
};
const invoiceDefinition: EntityDefinition<Invoice> = {
  id: "invoice",
  uniqueProperties: ["invoiceNumber"],
  indexedPropertyChains: [
    ["customerEmail"],
  ],
  _exampleEntityInstance: {} as Invoice,
};

// Create the EntityDb, using the definitions
const db = new EntityDb<Person | Invoice>({
  dbFilePath: "example-person-invoice.db",
  entityDefinitions: {
    person: personDefinition,
    invoice: invoiceDefinition,
  },
});

// Use the EntityDb
const alice: Person = {
  email: "alice@example.com",
  name: "Alice",
};
const invoice1: Invoice = {
  invoiceNumber: "1",
  customerEmail: "alice@example.com",
  amount: 100,
};

await db.save("person", alice);
await db.save("invoice", invoice1);

// Find the objects
const aliceFromDb: Person | undefined = await db.find(
  "person",
  "email",
  "alice@example.com",
);
console.log({ aliceFromDb });

const invoicesForAlice: Invoice[] | undefined = await db.findAll("invoice", [[
  "customerEmail",
  "alice@example.com",
]]);
console.log({ invoicesForAlice });
```

You may run the above example with:

```sh
deno run --unstable-kv --reload --allow-write=example-person-invoice.db --allow-read=example-person-invoice.db https://deno.land/x/kv_entity/readme/person-invoice.ts
```

For further usage examples, see the tests:

- [test/entity-db.test.ts](test/entity-db.test.ts)
