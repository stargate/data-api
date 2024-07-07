package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

/**
 * A Collection or Table the command works on
 */
public abstract class SchemaObject {

  public final SchemaObjectName name;

  protected SchemaObject(String keyspace, String name) {
    this(new SchemaObjectName(keyspace, name));
  }

  protected SchemaObject(SchemaObjectName name) {
    this.name = name;
  }
}
