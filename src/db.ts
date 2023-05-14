import { awaitAsyncIterableIterator, prop, VOID } from "./fn.ts";

/*
 * General comment about the design of this db:
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

/**
 * Type of some data object, that can be stored in the db.
 *
 * All the keys and values we care about are related to the db keys, so must be of type Deno.KvKeyPart.
 */
export type DataObject<
  T extends DataObject<T>,
> = {
  [K in keyof T]: K extends Deno.KvKeyPart ? (
      T[K] extends Deno.KvKeyPart ? T[K] : never
    )
    : never;
};

type EntityId = Deno.KvKeyPart & string;

/**
 * A description of something that can be stored in the db.
 */
export interface Entity<T extends DataObject<T>> {
  /** For example "person", "invoice", or "product" */
  id: EntityId;

  /** Unused instance of T, to help TypeScript infer types. */
  _exampleInstance: T;

  /** For example ["ssn", "emailAddress"]. These must be properties of T. */
  uniqueProperties: Array<keyof T>;

  /** For example [["lastname", "firstname"], ["country", "zipcode"]]. These must be chains of properties on T. */
  nonUniqueLookupPropertyChains: Array<keyof T>[];
}

type ExtractDataObjectType<E> = E extends Entity<infer T> ? T : never;
type ExtractEntityType<T> = T extends DataObject<infer T> ? Entity<T> : never;
type ExtractEntityId<T> = T extends DataObject<infer T>
  ? ExtractEntityType<T>["id"]
  : never;

/**
 * Defines a db, and how to store entities in it.
 * @param Ts The types of the entities that can be stored in the db.
 */
export interface DbConfig<
  Ts extends DataObject<Ts>,
> {
  /** The path to the file where the db is stored. If undefined, the default db is used. */
  dbFilePath?: string;
  /** The prefix to use for all keys in the db. */
  prefix?: Deno.KvKey;
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
    [I in ExtractEntityId<Ts>]:
      & ExtractEntityType<Ts>
      & { id: I };
  };
}

type DbConnectionCallback<T> = (db: Deno.Kv) => Promise<T> | T;

/**
 * Defines a db, and how to store entities in it.
 */
export class Db<
  Ts extends DataObject<Ts>,
> {
  /**
   * Configure a db.
   * @param config
   */
  constructor(
    private config: DbConfig<Ts>,
  ) {}

  /**
   * Save an entity value to the db.
   * @param entityId The id of the entity to save.
   * @param value The value to save.
   */
  async save<
    T extends Ts,
  >(
    entityId: ExtractEntityId<T>,
    value: T,
  ): Promise<void> {
    const keys: Deno.KvKey[] = this.getAllKeys(entityId, value);
    await this.doWithConnection(VOID, async (connection: Deno.Kv) => {
      const atomic = connection.atomic();
      for (const key of keys) {
        atomic.set(key, value);
      }
      await atomic.commit();
    });
  }

  async clearEntity<T extends Ts>(
    entityId: ExtractEntityId<T>,
  ): Promise<void> {
    const keys: Deno.KvKey[] = this.getAllKeys(entityId, {} as T);
    await this.doWithConnection(VOID, async (connection: Deno.Kv) => {
      const atomic = connection.atomic();
      for (const key of keys) {
        atomic.delete(key);
      }
      await atomic.commit();
    });
  }

  async clearAllEntities(): Promise<void> {
    await this.doWithConnection(VOID, async (connection: Deno.Kv) => {
      const atomic = connection.atomic();
      const entityIds: ExtractEntityId<Ts>[] = Object.keys(
        this.config.entities,
      ) as ExtractEntityId<Ts>[];
      for (const entityId of entityIds) {
        const keys: Deno.KvKey[] = this.getAllKeys(
          entityId,
          {} as Ts,
        );
        for (const key of keys) {
          atomic.delete(key);
        }
        await atomic.commit();
      }
    });
  }

  /**
   * Find an entity value in the db.
   * @param entityId The id of the entity to find.
   * @param uniquePropertyName The unique property to find the value for.
   * @param uniquePropertyValue The unique property value to find the value for.
   * @returns the value, or undefined if not found at the given key.
   */
  async find<
    T extends Ts,
  >(
    entityId: ExtractEntityId<T>,
    uniquePropertyName: keyof T,
    uniquePropertyValue: T[keyof T],
  ): Promise<T | undefined> {
    const key: Deno.KvKey = this.getUniqueKey(
      entityId,
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
   * @param entityId The id of the entity to find.
   * @param nonUniquePropertyChain The non-unique property chain to find values for.
   */
  async findAll<T extends Ts>(
    entityId: ExtractEntityId<T>,
    nonUniquePropertyChain: Array<Deno.KvKeyPart & keyof T>,
  ): Promise<T[]> {
    const key: Deno.KvKey = this.getNonUniqueKey(
      entityId,
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

  private async doWithConnection<
    T extends void | undefined | Ts | Ts[],
  >(
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
   * Calculate all the keys that an entity value is stored at.
   * @param entityId The id of the entity to calculate the keys for.
   * @param value The value to calculate the keys for.
   * @private
   */
  private getAllKeys<
    T extends Ts,
  >(
    entityId: ExtractEntityId<T>,
    value: T,
  ): Deno.KvKey[] {
    return [
      ...this.getUniqueKeys(entityId, value),
      ...this.getNonUniqueKeys(entityId),
    ];
  }

  /**
   * Calculate all the unique keys that an entity value is stored at.
   * @param entityId The id of the entity to calculate the keys for.
   * @param value The value to calculate the keys for.
   * @private
   */
  private getUniqueKeys<
    T extends Ts,
  >(
    entityId: ExtractEntityId<T>,
    value: T,
  ): Deno.KvKey[] {
    const entity: Entity<T> = this.config
      .entities[entityId] as unknown as Entity<T>;
    return entity.uniqueProperties.map((
      uniquePropertyName: keyof T,
    ) =>
      this.getUniqueKey(entityId, uniquePropertyName, value[uniquePropertyName])
    );
  }

  /**
   * Calculate the unique key that an entity value is stored at.
   * @param entityId The id of the entity to calculate the key for.
   * @param uniquePropertyName The unique property to calculate the key for.
   * @param uniquePropertyValue The unique property value to calculate the key for.
   * @private
   */
  private getUniqueKey<
    T extends Ts,
  >(
    entityId: ExtractEntityId<T>,
    uniquePropertyName: keyof T,
    uniquePropertyValue: T[keyof T],
  ): Deno.KvKey {
    return [
      ...(this.config.prefix ?? []),
      entityId,
      uniquePropertyName,
      uniquePropertyValue,
    ] as Deno.KvKey;
  }

  /**
   * Calculate all the non-unique keys that an entity's values are stored at.
   * @param entityId The id of the entity to calculate the keys for.
   * @private
   */
  private getNonUniqueKeys<
    T extends Ts,
  >(
    entityId: ExtractEntityId<T>,
  ): Deno.KvKey[] {
    const entity: Entity<T> = this.config
      .entities[entityId] as unknown as Entity<T>;
    return entity.nonUniqueLookupPropertyChains.map((
      propertyChain: (keyof T)[],
    ) => this.getNonUniqueKey(entityId, propertyChain));
  }

  /**
   * Calculate the non-unique key that an entity value is stored at.
   * @param entityId The id of the entity to calculate the key for.
   * @param propertyChain The property chain to calculate the key for.
   * @private
   */
  private getNonUniqueKey<
    T extends Ts,
    I extends Entity<T>["id"],
  >(
    entityId: I,
    propertyChain: (keyof T)[],
  ): Deno.KvKey {
    return [
      ...(this.config.prefix ?? []),
      entityId,
      ...propertyChain,
    ] as Deno.KvKey;
  }
}
