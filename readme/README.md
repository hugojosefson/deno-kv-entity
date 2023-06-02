# kv_entity

Typed library for specifying and storing entities in a
[Deno.Kv](https://deno.com/kv) database.

[![deno land](https://img.shields.io/badge/x/kv__entity-black.svg?logo=deno&labelColor=black)](https://deno.land/x/kv_entity)
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

For further usage examples, see the tests:

- [test/entity-db.test.ts](test/entity-db.test.ts)
