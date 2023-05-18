import { asArray, isKvKeyPart, prop, VOID } from "./fn.ts";

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
type EntityDefinitionId = Deno.KvKeyPart & string;

/**
 * A property on T, that is used to look up multiple instances of T.
 *
 * If used as part of a Deno.KvKey, will possibly be followed by a value of that property.
 */
type IndexedProperty<T extends EntityInstance<T>> = ExtractEntityDefinition<
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
  id: EntityDefinitionId;

  /** Unused instance of T, to help TypeScript infer types. */
  _exampleEntityInstance: T;

  /** For example ["ssn", "emailAddress"]. These must be properties of T. */
  uniqueProperties: Array<keyof T>;

  /** For example [["lastname", "firstname"], ["country", "zipcode"]]. These must be chains of properties on T. They will be used to construct Deno.KvKey's, for example ["lastname", "Doe", "firstname", "Alice"] */
  indexedPropertyChains: Array<Array<keyof T>>;
}

/**
 * Helper type to extract the EntityDefinition from an EntityInstance.
 *
 * For example: ExtractEntityDefinition<Person> === EntityDefinition<Person>
 */
type ExtractEntityDefinition<T> = T extends EntityInstance<infer T>
  ? EntityDefinition<T>
  : never;

/**
 * Helper type to extract the EntityDefinitionId from an EntityInstance.
 *
 * For example: ExtractEntityDefinitionId<Person> === "person"
 */
type ExtractEntityDefinitionId<T> = T extends EntityInstance<infer T>
  ? ExtractEntityDefinition<T>["id"]
  : never;

/**
 * Defines an EntityDb, and the structure of the entities that can be stored in it.
 * @param Ts The types of EntityInstance that can be stored in the db.
 */
export interface DbConfig<
  Ts extends EntityInstance<Ts>,
> {
  /** The path to the file where the db is stored. If undefined, the default db is used. */
  dbFilePath?: string;

  /** Any prefix to use for all keys in the db. */
  prefix?: Deno.KvKey;

  /**
   * The EntityDefinitions that define the structure of the entities that can be stored in the db.
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
   *       ["customerEmail"],
   *     ]
   *   }
   * }
   */
  entityDefinitions: {
    [I in ExtractEntityDefinitionId<Ts>]:
      & ExtractEntityDefinition<Ts>
      & { id: I };
  };
}

/**
 * A (possibly async) function that takes a Deno.Kv connection, and returns something of interest.
 */
type DbConnectionCallback<T> = (db: Deno.Kv) => Promise<T> | T;

/**
 * Defines an EntityDb, and how to store EntityInstances in it.
 */
export class EntityDb<Ts extends EntityInstance<Ts>> {
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
  async save<T extends Ts>(
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

  /**
   * Deletes all EntityInstance's for a given EntityDefinition.id.
   *
   * For example: deleteAll("person") will delete all Person's.
   *
   * @param entityDefinitionId The id of the EntityDefinition to delete all instances of.
   */
  async clearEntity<T extends Ts>(
    entityDefinitionId: ExtractEntityDefinitionId<T>,
  ): Promise<void> {
    if (typeof entityDefinitionId !== "string") {
      throw new Error(
        "EntityDefinition id must be a string. If you want to clear all entities, use clearAllEntities() instead.",
      );
    }
    await this._clearEntity(entityDefinitionId);
  }

  /**
   * Deletes all EntityInstance's known by this EntityDb
   */
  async clearAllEntities(): Promise<void> {
    await this._clearEntity();
  }

  private async _clearEntity<T extends Ts | never>(
    entityId?: ExtractEntityDefinitionId<T>,
  ): Promise<void> {
    const allKeys = this.getAllKeys(entityId);
    await this._doWithConnection(VOID, async (connection: Deno.Kv) => {
      const atomic = connection.atomic();
      for (const prefix of allKeys) {
        const kvEntries: Deno.KvEntry<unknown>[] = await asArray(
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
   * Find an EntityInstance in the db.
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
   * Find all EntityInstances in the db, that match the given non-unique property chain.
   * @param entityDefinitionId The id of the entity to find, if any. If undefined, all entities will be searched.
   * @param propertyLookupKey The non-unique property chain to find values for, if any. If undefined, all values for the given entity will be searched. Or, the name of a non-unique property, if only one property is to be searched.
   */
  async findAll<
    T extends Ts,
    K extends IndexedProperty<T>,
  >(
    entityDefinitionId?: ExtractEntityDefinitionId<T>,
    propertyLookupKey?: PropertyLookupPair<K, T>[] | K,
  ): Promise<T[]> {
    const key: Deno.KvKey = this.getNonUniqueKey(
      entityDefinitionId,
      propertyLookupKey,
    );
    return await this._doWithConnection(
      [] as T[],
      async (connection: Deno.Kv) => {
        const iterator: Deno.KvListIterator<T> = connection.list<T>({
          prefix: key,
        });
        const entries: Deno.KvEntry<T>[] = await asArray(
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
   * Calculate all the keys that an EntityInstance is stored at.
   * @param entityDefinitionId The id of the EntityDefinition to calculate the keys for, if any. If not provided, the keys will be calculated for all entities.
   * @param entityInstance The EntityInstance to calculate the keys for, if any. If not provided, the keys will be calculated for all under entityDefinitionId.
   * @private
   */
  private getAllKeys<
    T extends Ts,
  >(
    entityDefinitionId?: ExtractEntityDefinitionId<T>,
    entityInstance?: T,
  ): Deno.KvKey[] {
    if (typeof entityDefinitionId === "undefined") {
      return [this.getNonUniqueKey()];
    }
    if (typeof entityInstance === "undefined") {
      return [this.getNonUniqueKey(entityDefinitionId)];
    }
    return [
      ...this.getUniqueKeys(entityDefinitionId, entityInstance),
      ...this.getNonUniqueKeys(entityDefinitionId, entityInstance),
    ];
  }

  /**
   * Calculate all the unique keys that an EntityInstance is stored at.
   * @param entityDefinitionId The id of the EntityDefinition to calculate the keys for.
   * @param entityInstance The EntityInstance to calculate the keys for.
   * @private
   */
  private getUniqueKeys<
    T extends Ts,
  >(
    entityDefinitionId: ExtractEntityDefinitionId<T>,
    entityInstance: T,
  ): Deno.KvKey[] {
    const entity: EntityDefinition<T> = this.config
      .entityDefinitions[entityDefinitionId] as unknown as EntityDefinition<T>;
    return entity.uniqueProperties.map((
      uniqueProperty: keyof T,
    ) =>
      this.getUniqueKey(
        entityDefinitionId,
        uniqueProperty,
        entityInstance[uniqueProperty],
      )
    );
  }

  /**
   * Calculate the unique key that an EntityInstance is stored at.
   * @param entityDefinitionId The id of the entity to calculate the key for.
   * @param uniquePropertyName The unique property to calculate the key for.
   * @param uniquePropertyValue The unique property value to calculate the key for.
   * @private
   */
  private getUniqueKey<
    T extends Ts,
  >(
    entityDefinitionId: ExtractEntityDefinitionId<T>,
    uniquePropertyName: keyof T,
    uniquePropertyValue: T[keyof T],
  ): Deno.KvKey {
    return [
      ...(this.config.prefix ?? []),
      entityDefinitionId,
      uniquePropertyName,
      uniquePropertyValue,
    ] as Deno.KvKey;
  }

  /**
   * Calculate all the non-unique keys that an EntityInstance is stored at.
   * @param entityDefinitionId The id of the EntityDefinition to calculate the keys for.
   * @param entityInstance The EntityInstance to calculate the keys for.
   * @private
   */
  private getNonUniqueKeys<
    T extends Ts,
  >(
    entityDefinitionId: ExtractEntityDefinitionId<T>,
    entityInstance: T,
  ): Deno.KvKey[] {
    const entityDefinition: EntityDefinition<T> = this.config
      .entityDefinitions[entityDefinitionId] as unknown as EntityDefinition<T>;
    const indexedPropertyChains: Array<Array<keyof T>> =
      entityDefinition.indexedPropertyChains;
    const result: Deno.KvKey[] = [];

    for (const indexedPropertyChain of indexedPropertyChains) {
      const propertyLookupPairs = indexedPropertyChain.map((
        indexedProperty: keyof T,
      ) =>
        [
          indexedProperty,
          entityInstance[indexedProperty],
        ] as PropertyLookupPair<
          IndexedProperty<T>,
          T[IndexedProperty<T>]
        >
      );
      const key: Deno.KvKey = this.getNonUniqueKey(
        entityDefinitionId,
        propertyLookupPairs,
        entityInstance[entityDefinition.uniqueProperties[0]] as Deno.KvKeyPart,
      );
      result.push(key);
    }
    return result;
  }

  /**
   * Calculate the non-unique key that an EntityInstance is stored at.
   * @param entityDefinitionId The id of the entity to calculate the key for, if any. If not provided, all entities are targeted.
   * @param propertyLookupPairs The indexed property chain to calculate the key for, if any. If not provided, all indexed property chains are targeted.  Or, the name of an indexed property, if only one property is to be searched.
   * @param entityInstanceUniquePropertyValue The EntityInstance[uniqueProperties[0]] value to calculate the key for. If not provided, all EntityInstances are targeted.
   * @private
   */
  private getNonUniqueKey<
    T extends Ts,
    K extends IndexedProperty<T>,
  >(
    entityDefinitionId?: ExtractEntityDefinitionId<T>,
    propertyLookupPairs?:
      | PropertyLookupPair<K, T>[]
      | [...PropertyLookupPair<K, T>[], K]
      | K,
    entityInstanceUniquePropertyValue?: Deno.KvKeyPart,
  ): Deno.KvKey {
    return [
      ...(this.config.prefix ?? []),
      ...(typeof entityDefinitionId === "undefined"
        ? []
        : [entityDefinitionId]),
      ...(typeof propertyLookupPairs === "undefined"
        ? []
        : (isKvKeyPart(propertyLookupPairs)
          ? [propertyLookupPairs]
          : propertyLookupPairs.flat())),
      ...(typeof entityInstanceUniquePropertyValue === "undefined"
        ? []
        : [entityInstanceUniquePropertyValue]),
    ] as Deno.KvKey;
  }
}
