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
 * A tuple:
 *  The first element is a property name from T.
 *  The second element is the value of that property.
 *
 * T must be a DataObject.
 * K is the property of T.
 * T[K] is the type of the value at the property K.
 * The Entity<T>, which is calculated by ExtractEntityType<T>, must have the property K in its nonUniqueLookupPropertyChains.
 */
type PropertyLookupPair<
  K extends ExtractEntityType<
    T
  >["nonUniqueLookupPropertyChains"][number][number],
  T extends DataObject<T>,
> =
  & Deno.KvKey
  & [K, T[K]];

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

  /** For example [["lastname", "firstname"], ["country", "zipcode"]]. These must be chains of properties on T. They will be used to construct Deno.KvKey's, for example ["lastname", "Smith", "firstname", "Alice"] */
  nonUniqueLookupPropertyChains: Array<Array<keyof T>>;
}

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
    await this._doWithConnection(VOID, async (connection: Deno.Kv) => {
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
    if (typeof entityId !== "string") {
      throw new Error(
        "Entity id must be a string. If you want to clear all entities, use clearAllEntities() instead.",
      );
    }
    await this._clearEntity(entityId);
  }

  async clearAllEntities(): Promise<void> {
    await this._clearEntity();
  }

  private async _clearEntity<T extends Ts | never>(
    entityId?: ExtractEntityId<T>,
  ): Promise<void> {
    await this._doWithConnection(VOID, async (connection: Deno.Kv) => {
      const atomic = connection.atomic();
      for (const prefix of this.getAllKeys(entityId)) {
        console.error("Deleting prefix", prefix);
        const entries: Deno.KvEntry<unknown>[] =
          await awaitAsyncIterableIterator(connection.list({ prefix }));
        for (const entry of entries) {
          console.error("Deleting key", entry.key);
          atomic.delete(entry.key);
        }
      }
      await atomic.commit();
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
    return await this._doWithConnection(
      {} as T | undefined,
      async (connection: Deno.Kv) => {
        const entry: Deno.KvEntryMaybe<T> = await connection.get<T>(key);
        return entry.value ?? undefined;
      },
    );
  }

  /**
   * Find all entity values in the db, that match the given non-unique property chain.
   * @param entityId The id of the entity to find, if any. If undefined, all entities will be searched.
   * @param propertyLookupKey The non-unique property chain to find values for, if any. If undefined, all values for the given entity will be searched.
   */
  async findAll<
    T extends Ts,
    K extends ExtractEntityType<
      T
    >["nonUniqueLookupPropertyChains"][number][number],
  >(
    entityId?: ExtractEntityId<T>,
    propertyLookupKey?: PropertyLookupPair<K, T>[],
  ): Promise<T[]> {
    const key: Deno.KvKey = this.getNonUniqueKey(
      entityId,
      propertyLookupKey,
    );
    return await this._doWithConnection(
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

  async _doWithConnection<
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
   * @param entityId The id of the entity to calculate the keys for, if any. If not provided, the keys will be calculated for all entities.
   * @param value The value to calculate the keys for, if any. If not provided, the keys will be calculated for all entities.
   * @private
   */
  private getAllKeys<
    T extends Ts,
  >(
    entityId?: ExtractEntityId<T>,
    value?: T,
  ): Deno.KvKey[] {
    if (typeof entityId === "undefined") {
      return [this.getNonUniqueKey()];
    }
    if (typeof value === "undefined") {
      return [this.getNonUniqueKey(entityId)];
    }
    return [
      ...this.getUniqueKeys(entityId, value),
      ...this.getNonUniqueKeys(entityId, value),
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
   * @param value The value to calculate the keys for.
   * @private
   */
  private getNonUniqueKeys<
    T extends Ts,
  >(
    entityId: ExtractEntityId<T>,
    value: T,
  ): Deno.KvKey[] {
    const entity: Entity<T> = this.config
      .entities[entityId] as unknown as Entity<T>;
    const chains: Array<Array<keyof T>> = entity.nonUniqueLookupPropertyChains;
    const result: Deno.KvKey[] = [];
    for (const properties of chains) {
      const propertyLookupPairs = properties.map((property: keyof T) =>
        [
          property,
          value[property],
        ] as PropertyLookupPair<
          ExtractEntityType<T>["nonUniqueLookupPropertyChains"][number][number],
          T
        >
      );
      const key: Deno.KvKey = this.getNonUniqueKey(
        entityId,
        propertyLookupPairs,
        value[entity.uniqueProperties[0]] as Deno.KvKeyPart,
      );
      result.push(key);
    }
    return result;
  }

  /**
   * Calculate the non-unique key that an entity value is stored at.
   * @param entityId The id of the entity to calculate the key for, if any. If not provided, all entities are targeted.
   * @param propertyLookupPairs The non-unique property chain to calculate the key for, if any. If not provided, all non-unique property chains are targeted.
   * @param valueUniqueId The unique id of the value to calculate the key for. If not provided, all values are targeted.
   * @private
   */
  private getNonUniqueKey<
    T extends Ts,
    K extends ExtractEntityType<
      T
    >["nonUniqueLookupPropertyChains"][number][number],
  >(
    entityId?: ExtractEntityId<T>,
    propertyLookupPairs?: PropertyLookupPair<K, T>[],
    valueUniqueId?: Deno.KvKeyPart,
  ): Deno.KvKey {
    return [
      ...(this.config.prefix ?? []),
      ...(typeof entityId === "undefined" ? [] : [entityId]),
      ...(typeof propertyLookupPairs === "undefined"
        ? []
        : propertyLookupPairs.flat()),
      ...(typeof valueUniqueId === "undefined" ? [] : [valueUniqueId]),
    ] as Deno.KvKey;
  }
}
