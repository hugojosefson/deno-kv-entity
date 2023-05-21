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

/**
 * For example "person", "invoice", or "product".
 */
export type EntityDefinitionId = Deno.KvKeyPart & string;

/**
 * A property on T, that is used to look up multiple instances of T.
 *
 * If used as part of a Deno.KvKey, will possibly be followed by a value of that property.
 */
export type IndexedProperty<T extends EntityInstance<T>> =
  ExtractEntityDefinition<
    T
  >["indexedPropertyChains"][number][number];

/**
 * A tuple:
 *  The first element is an indexed property from T.
 *  The second element is the value of that property.
 *
 * T must be an EntityInstance.
 * T[IndexedProperty<T>] is the type of the value at the property IndexedProperty<T>.
 */
export type PropertyLookupPair<
  T extends EntityInstance<T>,
> =
  & Deno.KvKey
  & [IndexedProperty<T>, T[IndexedProperty<T>]];

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

  /**
   * For example [["lastname", "firstname"], ["country", "zipcode"]]. These must be chains of properties on T. They will
   * be used to construct Deno.KvKey's, for example ["lastname", "Doe", "firstname", "Alice"]
   */
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
export type ExtractEntityDefinitionId<T> = T extends EntityInstance<infer T>
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
   *
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
export type DbConnectionCallback<T> = (db: Deno.Kv) => Promise<T> | T;
