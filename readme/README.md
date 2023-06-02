# kv_entity

Typed library for specifying and storing entities in a
[Deno.Kv](https://deno.com/kv) database.

[![deno module](https://shield.deno.dev/x/kv_entity)](https://deno.land/x/kv_entity)
[![CI](https://github.com/hugojosefson/deno-kv-entity/actions/workflows/ci.yaml/badge.svg)](https://github.com/hugojosefson/deno-kv-entity/actions/workflows/ci.yaml)

## Requirements

Requires [Deno](https://deno.land/) v1.32 or later, with the `--unstable` flag.

## API

Please see the
[auto-generated API documentation](https://deno.land/x/kv_entity?doc).

## Example usage

```typescript
"@@include(./person-invoice.ts)";
```

You may run the above example with:

```sh
deno run --unstable --reload --allow-write=example-person-invoice.db --allow-read=example-person-invoice.db https://deno.land/x/kv_entity/readme/person-invoice.ts
```

For further usage examples, see the tests:

- [test/entity-db.test.ts](test/entity-db.test.ts)
