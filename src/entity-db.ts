import { awaitAsyncIterableIterator, isKvKeyPart, prop, VOID } from "./fn.ts";

/*
 * General comment about the design of this EntityDb:
 *
 * uniqueProperty is related to a Deno.KvKey that can be used to look up a single EntityInstance.
 * indexedPropertyChain is related to a Deno.KvKey that can be used to lookup multiple EntityInstance.
 *
 * An EntityInstance is stored directly on each of the unique Deno.KvKey's derived from its EntityDefinition's uniqueProperties.
 * For each indexedPropertyChain, the Deno.KvKey to store the EntityInstance at, is calculated as:
 * [
 *   ...indexedPropertyChain.flatMap(prop => [prop, entityInstance[prop]]),
 *   EntityDefinition.uniqueProperties[0]
 * ].
 *
 * For example, if we have an EntityDefinition with id "person", and
 * uniqueProperties ["ssn", "emailAddress"], and
 * indexedPropertyChains [["lastname", "firstname"], ["country", "zipcode"]], then
 * we store the EntityInstance at the following keys:
 * - ["person", "ssn", "123456789"]
 * - ["person", "emailAddress", "alice@example.com"]
 * - ["person", "lastname", "Doe", "firstname", "Alice", "123456789"]
 * - ["person", "country", "US", "zipcode", "12345", "123456789"]
 *
 * The first two represent unique keys for an EntityInstance, and the last two represent indexed keys for that EntityInstance.
 */

/**
 * An EntityInstance, the concrete object that can be stored in the db.
 *
 * For example: {"firstname": "Alice", "lastname": "Doe", "ssn": "123456789", "emailAddress": "alice@example.com"}
 *
 * All the keys and values we care about are related to the db keys, so must be of type Deno.KvKeyPart.
 */
export type EntityInstance<T extends EntityInstance<T>> = {
  [K in keyof T]: K extends Deno.KvKeyPart ? (
      T[K] extends Deno.KvKeyPart ? T[K] : never
    )
    : never;
};

/** For example "person", "invoice", or "product" */
type EntityId = Deno.KvKeyPart & string;

/**
 * A property on T, that is used to look up multiple instances of T.
 *
 * If used as part of a Deno.KvKey, will possibly be followed by a value of that property.
 */
type IndexedProperty<T extends EntityInstance<T>> = ExtractEntityType<
  T
>["indexedPropertyChains"][number][number];

/**
 * A tuple:
 *  The first element is an indexed property from T.
 *  The second element is the value of that property.
 *
 * T must be an EntityInstance.
 * K is the indexed property of T.
 * T[K] is the type of the value at the property K.
 */
type PropertyLookupPair<
  K extends IndexedProperty<T>,
  T extends EntityInstance<T>,
> =
  & Deno.KvKey
  & [K, T[K]];

/**
 * A definition of an Entity that can be stored in the db.
 */
export interface EntityDefinition<T extends EntityInstance<T>> {
  /** For example "person", "invoice", or "product" */
  id: EntityId;

  /** Unused instance of T, to help TypeScript infer types. */
  _exampleInstance: T;

  /** For example ["ssn", "emailAddress"]. These must be properties of T. */
  uniqueProperties: Array<keyof T>;

  /** For example [["lastname", "firstname"], ["country", "zipcode"]]. These must be chains of properties on T. They will be used to construct Deno.KvKey's, for example ["lastname", "Doe", "firstname", "Alice"] */
  indexedPropertyChains: Array<Array<keyof T>>;
}

type ExtractEntityType<T> = T extends EntityInstance<infer T>
  ? EntityDefinition<T>
  : never;
type ExtractEntityDefinitionId<T> = T extends EntityInstance<infer T>
  ? ExtractEntityType<T>["id"]
  : never;

/**
 * Defines a db, and how to store entities in it.
 * @param Ts The types of the entities that can be stored in the db.
 */
export interface DbConfig<
  Ts extends EntityInstance<Ts>,
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
   *     indexedPropertyChains: [
   *       ["lastname", "firstname"],
   *       ["country", "zipcode"]
   *     ]
   *   },
   *   invoice: {
   *     id: "invoice",
   *     uniqueProperties: ["invoiceNumber"],
   *     indexedPropertyChains: [
   *       ["customer", "lastname", "firstname"],
   *       ["customer", "country", "zipcode"]
   *     ]
   *   }
   * }
   */
  entityDefinitions: {
    [I in ExtractEntityDefinitionId<Ts>]:
      & ExtractEntityType<Ts>
      & { id: I };
  };
}

type DbConnectionCallback<T> = (db: Deno.Kv) => Promise<T> | T;

/**
 * Defines a db, and how to store entities in it.
 */
export class EntityDb<
  Ts extends EntityInstance<Ts>,
> {
  /**
   * Configure a db.
   * @param config
   */
  constructor(
    private config: DbConfig<Ts>,
  ) {}

  /**
   * Save an EntityInstance to the db.
   * @param entityDefinitionId The id of the EntityDefinition to save the value to.
   * @param entityInstance The EntityInstance to save.
   */
  async save<
    T extends Ts,
  >(
    entityDefinitionId: ExtractEntityDefinitionId<T>,
    entityInstance: T,
  ): Promise<void> {
    const keys: Deno.KvKey[] = this.getAllKeys(
      entityDefinitionId,
      entityInstance,
    );
    await this._doWithConnection(VOID, async (connection: Deno.Kv) => {
      const atomic = connection.atomic();
      for (const key of keys) {
        atomic.set(key, entityInstance);
      }
      await atomic.commit();
    });
  }

  async clearEntity<T extends Ts>(
    entityId: ExtractEntityDefinitionId<T>,
  ): Promise<void> {
    if (typeof entityId !== "string") {
      throw new Error(
        "EntityDefinition id must be a string. If you want to clear all entities, use clearAllEntities() instead.",
      );
    }
    await this._clearEntity(entityId);
  }

  async clearAllEntities(): Promise<void> {
    await this._clearEntity();
  }

  private async _clearEntity<T extends Ts | never>(
    entityId?: ExtractEntityDefinitionId<T>,
  ): Promise<void> {
    await this._doWithConnection(VOID, async (connection: Deno.Kv) => {
      const atomic = connection.atomic();
      for (const prefix of this.getAllKeys(entityId)) {
        const kvEntries: Deno.KvEntry<unknown>[] =
          await awaitAsyncIterableIterator(
            connection.list({ prefix }),
          );
        for (const { key } of kvEntries) {
          atomic.delete(key);
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
    entityId: ExtractEntityDefinitionId<T>,
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
   * @param propertyLookupKey The non-unique property chain to find values for, if any. If undefined, all values for the given entity will be searched. Or, the name of a non-unique property, if only one property is to be searched.
   */
  async findAll<
    T extends Ts,
    K extends IndexedProperty<T>,
  >(
    entityId?: ExtractEntityDefinitionId<T>,
    propertyLookupKey?: PropertyLookupPair<K, T>[] | K,
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
    entityId?: ExtractEntityDefinitionId<T>,
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
    entityId: ExtractEntityDefinitionId<T>,
    value: T,
  ): Deno.KvKey[] {
    const entity: EntityDefinition<T> = this.config
      .entityDefinitions[entityId] as unknown as EntityDefinition<T>;
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
    entityId: ExtractEntityDefinitionId<T>,
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
    entityId: ExtractEntityDefinitionId<T>,
    value: T,
  ): Deno.KvKey[] {
    const entity: EntityDefinition<T> = this.config
      .entityDefinitions[entityId] as unknown as EntityDefinition<T>;
    const chains: Array<Array<keyof T>> = entity.indexedPropertyChains;
    const result: Deno.KvKey[] = [];
    for (const properties of chains) {
      const propertyLookupPairs = properties.map((property: keyof T) =>
        [
          property,
          value[property],
        ] as PropertyLookupPair<
          IndexedProperty<T>,
          T[IndexedProperty<T>]
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
   * @param propertyLookupPairs The non-unique property chain to calculate the key for, if any. If not provided, all non-unique property chains are targeted.  Or, the name of a non-unique property, if only one property is to be searched.
   * @param valueUniqueId The unique id of the value to calculate the key for. If not provided, all values are targeted.
   * @private
   */
  private getNonUniqueKey<
    T extends Ts,
    K extends IndexedProperty<T>,
  >(
    entityId?: ExtractEntityDefinitionId<T>,
    propertyLookupPairs?:
      | PropertyLookupPair<K, T>[]
      | [...PropertyLookupPair<K, T>[], K]
      | K,
    valueUniqueId?: Deno.KvKeyPart,
  ): Deno.KvKey {
    return [
      ...(this.config.prefix ?? []),
      ...(typeof entityId === "undefined" ? [] : [entityId]),
      ...(typeof propertyLookupPairs === "undefined"
        ? []
        : (isKvKeyPart(propertyLookupPairs)
          ? [propertyLookupPairs]
          : propertyLookupPairs.flat())),
      ...(typeof valueUniqueId === "undefined" ? [] : [valueUniqueId]),
    ] as Deno.KvKey;
  }
}
