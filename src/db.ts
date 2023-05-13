import { awaitAsyncIterableIterator, prop } from "./fn.ts";

/** An instance of something of the type void. */
const VOID: void = undefined as void;

// TODO: manipulate single property of T, under several keys, as one transaction

/**
 * Mutate a value in the db.
 * @param connection The db connection.
 * @param key The key to the value to mutate.
 * @param fn The function to mutate the value with. Return the mutated value.
 * @returns true if the value was mutated, false if the value was not found.
 */
export async function mutateValue<T extends DataObject>(
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
 * Type of some data object, that can be stored in the db.
 */
export type DataObject = { [key: string & Deno.KvKeyPart]: Deno.KvKeyPart };

/**
 * A description of something that can be stored in the db.
 */
export interface Entity<T extends DataObject> {
  /** For example "person", "invoice", or "product" */
  id: string;

  /** For example ["ssn", "emailAddress"]. These must be properties of T. */
  uniqueProperties: Array<Deno.KvKeyPart & keyof T>;

  /** For example [["lastname", "firstname"], ["country", "zipcode"]]. These must be chains of properties on T. */
  nonUniqueLookupPropertyChains: Array<Array<Deno.KvKeyPart & keyof T>>;
}

/**
 * Defines a db, and how to store entities in it.
 * @param Es The ids of the entities that can be stored in the db.
 * @param Ts The types of the entities that can be stored in the db.
 */
export interface DbConfig<Es extends string, Ts extends DataObject> {
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

export type DbConnectionCallback<T> = (db: Deno.Kv) => Promise<T> | T;

/**
 * Defines a db, and how to store entities in it.
 */
export class Db<Es extends string, Ts extends DataObject> {
  constructor(private readonly config: DbConfig<Es, Ts>) {}

  async doWithConnection<T extends void | undefined | Ts | Ts[]>(
    _expectedReturnType: T,
    fn: DbConnectionCallback<T>,
  ): Promise<T> {
    const connection: Deno.Kv = await Deno.openKv(this.config.dbFilePath);
    try {
      return await fn(connection);
    } finally {
      connection.close();
    }
  }

  /**
   * Save an entity value to the db.
   * @param entity The entity to save the value for.
   * @param value The value to save.
   */
  async save<T extends Ts>(entity: Entity<T>, value: T): Promise<void> {
    const keys: Deno.KvKey[] = this.getAllKeys(entity, value);
    await this.doWithConnection(VOID, async (connection: Deno.Kv) => {
      const atomic = connection.atomic();
      for (const key of keys) {
        atomic.set(key, value);
      }
      await atomic.commit();
    });
  }

  /**
   * Find an entity value in the db.
   * @param entity The entity to find the value for.
   * @param uniquePropertyName The unique property to find the value for.
   * @param uniquePropertyValue The unique property value to find the value for.
   * @returns the value, or undefined if not found at the given key.
   */
  async find<T extends Ts>(
    entity: Entity<T>,
    uniquePropertyName: Deno.KvKeyPart & keyof T,
    uniquePropertyValue: T[keyof T],
  ): Promise<T | undefined> {
    const key: Deno.KvKey = this.getUniqueKey(
      entity,
      uniquePropertyName,
      uniquePropertyValue,
    );
    return await this.doWithConnection(
      {} as T | undefined,
      async (connection: Deno.Kv) => {
        const entry: Deno.KvEntryMaybe<T> = await connection.get<T>(key);
        return entry.value ?? undefined;
      },
    );
  }

  /**
   * Find all entity values in the db, that match the given non-unique property chain.
   * @param entity The entity to find values for.
   * @param nonUniquePropertyChain The non-unique property chain to find values for.
   */
  async findAll<T extends Ts>(
    entity: Entity<T>,
    nonUniquePropertyChain: Array<Deno.KvKeyPart & keyof T>,
  ): Promise<T[]> {
    const key: Deno.KvKey = this.getNonUniqueKey(
      entity,
      nonUniquePropertyChain,
    );
    return await this.doWithConnection(
      [] as T[],
      async (connection: Deno.Kv) => {
        const iterator: Deno.KvListIterator<T> = connection.list<T>({
          prefix: key,
        });
        const entries: Deno.KvEntry<T>[] = await awaitAsyncIterableIterator(
          iterator,
        );
        return entries.map(prop("value")) as T[];
      },
    );
  }

  /**
   * Calculate all the keys that an entity value is stored at.
   * @param entity The entity to calculate the keys for.
   * @param value The value to calculate the keys for.
   * @private
   */
  private getAllKeys<T extends Ts>(
    entity: Entity<T>,
    value: T,
  ): Deno.KvKey[] {
    return [
      ...this.getUniqueKeys(entity, value),
      ...this.getNonUniqueKeys(entity),
    ];
  }

  /**
   * Calculate all the unique keys that an entity value is stored at.
   * @param entity The entity to calculate the keys for.
   * @param value The value to calculate the keys for.
   * @private
   */
  private getUniqueKeys<T extends Ts>(
    entity: Entity<T>,
    value: T,
  ): Deno.KvKey[] {
    return entity.uniqueProperties.map((
      uniquePropertyName: Deno.KvKeyPart & keyof T,
    ) =>
      this.getUniqueKey(entity, uniquePropertyName, value[uniquePropertyName])
    );
  }

  /**
   * Calculate the unique key that an entity value is stored at.
   * @param entity The entity to calculate the key for.
   * @param uniquePropertyName The unique property to calculate the key for.
   * @param uniquePropertyValue The unique property value to calculate the key for.
   * @private
   */
  private getUniqueKey<T extends Ts>(
    entity: Entity<T>,
    uniquePropertyName: Deno.KvKeyPart & keyof T,
    uniquePropertyValue: T[keyof T],
  ): Deno.KvKey {
    return [entity.id, uniquePropertyName, uniquePropertyValue];
  }

  /**
   * Calculate all the non-unique keys that an entity's values are stored at.
   * @param entity The entity to calculate the keys for.
   * @private
   */
  private getNonUniqueKeys<T extends Ts>(
    entity: Entity<T>,
  ): Deno.KvKey[] {
    return entity.nonUniqueLookupPropertyChains.map((
      propertyChain: (Deno.KvKeyPart & keyof T)[],
    ) => this.getNonUniqueKey(entity, propertyChain));
  }

  /**
   * Calculate the non-unique key that an entity value is stored at.
   * @param entity The entity to calculate the key for.
   * @param propertyChain The property chain to calculate the key for.
   * @private
   */
  private getNonUniqueKey<T extends Ts>(
    entity: Entity<T>,
    propertyChain: (Deno.KvKeyPart & keyof T)[],
  ): Deno.KvKey {
    return [entity.id, ...propertyChain];
  }
}
