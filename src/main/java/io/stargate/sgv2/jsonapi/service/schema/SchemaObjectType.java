package io.stargate.sgv2.jsonapi.service.schema;

import java.util.Objects;

/** The types of schema object that are used in the API */
public enum SchemaObjectType {
  COLLECTION(Constants.COLLECTION),
  DATABASE(Constants.DATABASE),
  INDEX(Constants.INDEX),
  KEYSPACE(Constants.KEYSPACE),
  TABLE(Constants.TABLE),
  UDT(Constants.UDT);

  /** Constants so the public HTTP API objects can use the same values. */
  public interface Constants {
    String COLLECTION = "Collection";
    String DATABASE = "Database";
    String INDEX = "Index";
    String KEYSPACE = "Keyspace";
    String TABLE = "Table";
    String UDT = "Udt";
  }

  private final String apiName;

  SchemaObjectType(String apiName) {
    this.apiName = Objects.requireNonNull(apiName, "apiName must not be null");
  }

  /** Gets the name to use when identifying this schema object type in the public API. */
  public String apiName() {
    return apiName;
  }
}
