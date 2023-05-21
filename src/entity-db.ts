import { asArray, isDefined, isKvKeyPart, Maybe, prop, VOID } from "./fn.ts";
import {
  DbConfig,
  DbConnectionCallback,
  EntityDefinition,
  EntityInstance,
  ExtractEntityDefinitionId,
  IndexedProperty,
  PropertyLookupPair,
} from "./types.ts";

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
      const atomic: Deno.AtomicOperation = connection.atomic();
      for (const key of keys) {
        atomic.set(key, entityInstance);
      }
      const { ok } = await atomic.commit();
      if (!ok) {
        throw new Error("commit failed");
      }
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
      const atomic: Deno.AtomicOperation = connection.atomic();
      for (const prefix of allKeys) {
        const kvEntries: Deno.KvEntry<unknown>[] = await asArray(
          connection.list({ prefix }),
        );
        for (const { key } of kvEntries) {
          atomic.delete(key);
        }
      }

      const { ok } = await atomic.commit();
      if (!ok) {
        throw new Error("commit failed");
      }
    });
  }

  /**
   * Find an EntityInstance in the db.
   * @param entityDefinitionId The id of the EntityDefinition to find the EntityInstance for.
   * @param uniquePropertyName The unique property to find the EntityInstance for.
   * @param uniquePropertyValue The unique property value to find the EntityInstance for.
   * @returns the EntityInstance, or undefined if not found at the given key.
   */
  async find<
    T extends Ts,
  >(
    entityDefinitionId: ExtractEntityDefinitionId<T>,
    uniquePropertyName: keyof T,
    uniquePropertyValue: T[keyof T],
  ): Promise<Maybe<T>> {
    const key: Deno.KvKey = this.getUniqueKey(
      entityDefinitionId,
      uniquePropertyName,
      uniquePropertyValue,
    );
    return await this._doWithConnection(
      {} as Maybe<T>,
      async (connection: Deno.Kv) => {
        const entry: Deno.KvEntryMaybe<T> = await connection.get<T>(key);
        return entry.value ?? undefined;
      },
    );
  }

  /**
   * Delete an EntityInstance from the db.
   * @param entityDefinitionId The id of the EntityDefinition to delete the EntityInstance from.
   * @param uniquePropertyName The unique property to delete the EntityInstance for.
   * @param uniquePropertyValue The unique property value to delete the EntityInstance for.
   */
  async delete<
    T extends Ts,
  >(
    entityDefinitionId: ExtractEntityDefinitionId<T>,
    uniquePropertyName: keyof T,
    uniquePropertyValue: T[keyof T],
  ): Promise<void> {
    const entityInstance: Maybe<T> = await this.find(
      entityDefinitionId,
      uniquePropertyName,
      uniquePropertyValue,
    );

    if (isDefined(entityInstance)) {
      await this.deleteEntityInstance(entityDefinitionId, entityInstance);
    }
  }

  async deleteEntityInstance<T extends Ts>(
    entityDefinitionId: ExtractEntityDefinitionId<T>,
    entityInstance: T,
  ): Promise<void> {
    // find all keys the same way as when saving
    const keys: Deno.KvKey[] = this.getAllKeys(
      entityDefinitionId,
      entityInstance,
    );
    await this._doWithConnection(VOID, async (connection: Deno.Kv) => {
      const atomic: Deno.AtomicOperation = connection.atomic();
      for (const key of keys) {
        atomic.delete(key);
      }
      const { ok } = await atomic.commit();
      if (!ok) {
        throw new Error("commit failed");
      }
    });
  }

  /**
   * Find all EntityInstances in the db, that match the given non-unique property chain.
   * @param entityDefinitionId The id of the entity to find, if any. If undefined, all entities will be searched.
   * @param propertyLookupKey The non-unique property chain to find values for, if any. If undefined, all values for the given entity will be searched. Or, the name of a non-unique property, if only one property is to be searched.
   */
  async findAll<T extends Ts>(
    entityDefinitionId?: ExtractEntityDefinitionId<T>,
    propertyLookupKey?: PropertyLookupPair<T>[] | IndexedProperty<T>,
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
    if (entityDefinitionId === undefined) {
      return [this.getNonUniqueKey()];
    }
    if (entityInstance === undefined) {
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
    const entityDefinition: EntityDefinition<T> = this.config
      .entityDefinitions[entityDefinitionId] as unknown as EntityDefinition<T>;
    return entityDefinition.uniqueProperties.map((
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
    const uniqueProperty: keyof T = entityDefinition.uniqueProperties[0];
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
        ] as PropertyLookupPair<T>
      );
      const nonUniqueKey: Deno.KvKey = this.getNonUniqueKey(
        entityDefinitionId,
        propertyLookupPairs,
        entityInstance[uniqueProperty] as Deno.KvKeyPart,
      );
      result.push(nonUniqueKey);
    }
    return result;
  }

  /**
   * Calculate the non-unique key that an EntityInstance is stored at.
   * @param entityDefinitionId The id of the entity to calculate the key for, if any. If not provided, all entities are targeted.
   * @param propertyLookup The indexed property chain to calculate the key for, if any. If not provided, all indexed property chains are targeted.  Or, the name of an indexed property, if only one property is to be searched.
   * @param entityInstanceUniquePropertyValue The EntityInstance[uniqueProperties[0]] value to calculate the key for. If not provided, all EntityInstances are targeted.
   * @private
   */
  private getNonUniqueKey<T extends Ts>(
    entityDefinitionId?: ExtractEntityDefinitionId<T>,
    propertyLookup?:
      | PropertyLookupPair<T>[]
      | [...PropertyLookupPair<T>[], IndexedProperty<T>]
      | IndexedProperty<T>,
    entityInstanceUniquePropertyValue?: Deno.KvKeyPart,
  ): Deno.KvKeyPart[] {
    const result: Deno.KvKeyPart[] = [];

    if (isDefined(this.config.prefix)) {
      result.push(...this.config.prefix);
    }

    if (isDefined(entityDefinitionId)) {
      result.push(entityDefinitionId);
    } else {
      if ([propertyLookup, entityInstanceUniquePropertyValue].some(isDefined)) {
        throw new Error(
          "entityDefinitionId must be provided if propertyLookup or entityInstanceUniquePropertyValue are provided",
        );
      }
    }

    if (isDefined(propertyLookup)) {
      if (isKvKeyPart(propertyLookup)) { // if it's a single property, an IndexedProperty<T>
        result.push(propertyLookup);
      } else { // it's an array of PropertyLookupPair<T> or an array of PropertyLookupPair<T> and an IndexedProperty<T>
        result.push(...propertyLookup.flat() as Deno.KvKeyPart[]);
      }
    } else {
      if (isDefined(entityInstanceUniquePropertyValue)) {
        throw new Error(
          "propertyLookup must be provided if entityInstanceUniquePropertyValue is provided",
        );
      }
    }

    if (isDefined(entityInstanceUniquePropertyValue)) {
      result.push(entityInstanceUniquePropertyValue);
    }

    return result;
  }
}
