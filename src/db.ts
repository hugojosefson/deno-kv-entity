// import { awaitAsyncIterableIterator } from "./fn.ts";

// TODO: save a T under several keys, as one transaction
export async function saveToMultipleKeys<T extends DataType>(
  connection: Deno.Kv,
  keys: Deno.KvKey[],
  value: T,
): Promise<void> {
  const atomic = connection.atomic();
  for (const key of keys) {
    atomic.set(key, value);
  }
  await atomic.commit();
}

// TODO: manipulate single property of T, under several keys, as one transaction

// TODO: encapsulate the structure of a db, the the keys to store things under, and how they can be looked up.
// TODO: probably the structure (type) of T, so we know which properties are available to look it up under.
// TODO: only lookup-able with unique properties?
// TODO: listable with non-unique properties?

/**
 * Mutate a value in the db.
 * @param connection The db connection.
 * @param key The key to the value to mutate.
 * @param fn The function to mutate the value with. Return the mutated value.
 * @returns true if the value was mutated, false if the value was not found.
 */
export async function mutateValue<T extends DataType>(
  connection: Deno.Kv,
  key: Deno.KvKey,
  fn: (value: T) => Promise<T> | T,
): Promise<boolean> {
  const entry: Deno.KvEntryMaybe<T> = await connection.get<T>(key);
  if (!entry.versionstamp) {
    return false;
  }
  const newValue: T = await fn(entry.value);
  connection.atomic().check(entry).set(key, newValue).commit();
  return true;
}

/**
 * The keys that we store an entity under.
 *
 * unique denotes a key that can be used to look up a single entity.
 * nonUnique denotes a key that can be used to lookup multiple entities.
 *
 * A value is stored directly on each of the unique keys.
 * For each nonUnique key, the key to store the value at, is calculated as: [...nonUniqueKey, entity.uniqueProperties[0]].
 *
 * For example, if we have an entity with id "person", and
 * uniqueProperties ["ssn", "emailAddress"], and
 * nonUniqueLookupPropertyChains [["lastname", "firstname"], ["country", "zipcode"]], then
 * we store the value at the following keys:
 * - ["person", "ssn", "123456789"]
 * - ["person", "emailAddress", "alice@example.com"]
 * - ["person", "lastname", "Doe", "firstname", "Alice", "123456789"]
 * - ["person", "country", "US", "zipcode", "12345", "123456789"]
 *
 * The first two are unique keys, and the last two are non-unique keys.
 */
export interface EntityKeys {
  unique: Deno.KvKey[];
  nonUnique: Deno.KvKey[];
}

/**
 * Type of something that can be stored in the db.
 */
export type DataType = Record<string, unknown>;

/**
 * A description of something that can be stored in the db.
 */
export interface Entity<T extends DataType> {
  /** For example "person", "invoice", or "product" */
  id: string;

  /** For example ["ssn", "emailAddress"]. These must be properties of T. */
  uniqueProperties: Array<keyof T>;

  /** For example [["lastname", "firstname"], ["country", "zipcode"]]. These must be chains of properties on T. */
  nonUniqueLookupPropertyChains: Array<Array<keyof T>>;
}

/**
 * Defines a db, and how to store entities in it.
 * @param Es The ids of the entities that can be stored in the db.
 * @param Ts The types of the entities that can be stored in the db.
 */
export interface DbConfig<Es extends string, Ts extends DataType> {
  /** The path to the file where the db is stored. If undefined, the default db is used. */
  dbFilePath?: string;
  /**
   * The entities that can be stored in the db.
   * Example: {
   *   person: {
   *     id: "person",
   *     uniqueProperties: ["ssn", "emailAddress"],
   *     nonUniqueLookupPropertyChains: [
   *       ["lastname", "firstname"],
   *       ["country", "zipcode"]
   *     ]
   *   },
   *   invoice: {
   *     id: "invoice",
   *     uniqueProperties: ["invoiceNumber"],
   *     nonUniqueLookupPropertyChains: [
   *       ["customer", "lastname", "firstname"],
   *       ["customer", "country", "zipcode"]
   *     ]
   *   }
   * }
   */
  entities: {
    [K in Es]: Entity<Ts> & { id: K };
  };
}

export type DbConnectionCallback = (db: Deno.Kv) => Promise<void> | void;

export type DbConnectionCallbackWithReturn<T extends DataType> = (
  db: Deno.Kv,
) => Promise<T> | T;

/**
 * Defines a db, and how to store entities in it.
 */
export class Db<Es extends string, Ts extends DataType> {
  constructor(private readonly config: DbConfig<Es, Ts>) {}

  async doWithConnectionWithReturn<T extends Ts>(
    fn: DbConnectionCallbackWithReturn<T>,
  ): Promise<T> {
    const connection: Deno.Kv = await Deno.openKv(this.config.dbFilePath);
    try {
      return await fn(connection);
    } finally {
      connection.close();
    }
  }

  async doWithConnection(
    fn: DbConnectionCallback,
  ): Promise<void> {
    const connection: Deno.Kv = await Deno.openKv(this.config.dbFilePath);
    try {
      await fn(connection);
    } finally {
      connection.close();
    }
  }

  /**
   * Save an entity value to the db.
   * @param entity
   * @param value
   */
  async save<T extends Ts>(entity: Entity<T>, value: T): Promise<void> {
    const keys: Deno.KvKey[] = this.getAllKeys(entity, value);
    await this.doWithConnection(async (connection: Deno.Kv) => {
      await saveToMultipleKeys(connection, keys, value);
    });
  }
  //
  // /**
  //  * Lookup an entity value in the db.
  //  * @param entity
  //  * @param uniquePropertyName
  //  * @param uniquePropertyValue
  //  * @returns the value, or undefined if not found
  //  */
  // async lookup<
  //   E extends Entity<T> & { uniqueProperties: [P] },
  //   T extends { [uniquePropertyName: P]: V },
  //   V,
  //   P,
  // >(
  //   entity: E,
  //   uniquePropertyName: P,
  //   uniquePropertyValue: V,
  // ): Promise<T | undefined> {
  //   const key = this.getUniqueKey(
  //     entity,
  //     uniquePropertyName,
  //     uniquePropertyValue,
  //   );
  //   return await this.doWithConnection(async (connection: Deno.Kv) => {
  //     const entry = await connection.get<T>(key);
  //     return entry.value ?? undefined;
  //   });
  // }
  //
  // /**
  //  * Lookup several entity values in the db.
  //  * @param entity
  //  * @param nonUniquePropertyName
  //  * @param nonUniquePropertyValue
  //  * @returns the values, or an empty array if not found
  //  */
  // async lookupAll<
  //   E extends Entity<T> & { nonUniqueLookupPropertyChains: [[P]] },
  //   T extends { [nonUniquePropertyName: P]: V },
  //   V,
  //   P,
  // >(
  //   entity: E,
  //   nonUniquePropertyName: P,
  //   nonUniquePropertyValue: V,
  // ): Promise<T[]> {
  //   const key = this.getNonUniqueKey(
  //     entity,
  //     nonUniquePropertyName,
  //     nonUniquePropertyValue,
  //   );
  //   return await this.doWithConnection(async (connection: Deno.Kv) => {
  //     const iterator = connection.list<T>({ prefix: key });
  //     const entries = await awaitAsyncIterableIterator(iterator);
  //     return entries.map((entry) => entry.value);
  //   });
  // }
  //
  // /**
  //  * Deletes an entity value from all the keys it is stored under, atomically.
  //  * @param entity
  //  * @param value
  //  */
  // async delete<T>(entity: Entity<T extends Ts>, value: T): Promise<void> {
  //   const keys = this.getAllKeys(entity, value);
  //   await this.doWithConnection(async (connection: Deno.Kv) => {
  //     const atomic = connection.atomic();
  //     for (const key of keys) {
  //       atomic.delete(key);
  //     }
  //     await atomic.commit();
  //   });
  // }

  private getAllKeys<T extends Ts>(
    _entity: Entity<T>,
    _value: T,
  ): Deno.KvKey[] {
    return []; // TODO
  }
}
