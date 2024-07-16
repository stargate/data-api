package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

/** A Collection or Table the command works on */
public abstract class SchemaObject {

  // Because a lot of code needs to make decisions based on the type of the SchemaObject use an
  // enum and we also have generics for strong type checking
  public enum SchemaObjectType {
    TABLE,
    COLLECTION,
    KEYSPACE,
  }

  public final SchemaObjectType type;
  public final SchemaObjectName name;

  protected SchemaObject(SchemaObjectType type, SchemaObjectName name) {
    this.type = type;
    this.name = name;
  }

  /**
   * Sublcasses must always return an instance of VectorConfig, if there is no vector config they
   * should return VectorConfig.notEnabledVectorConfig()
   *
   * @return
   */
  public abstract VectorConfig vectorConfig();

  /**
   * Call to get an instance of the appropriate {@link IndexUsage} for this schema object
   *
   * @return non null, IndexUsage instance
   */
  public abstract IndexUsage newIndexUsage();
}
