package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;
import java.util.Objects;

/** A Collection or Table the command works on */
public abstract class SchemaObject implements PrettyPrintable {

  // Because a lot of code needs to make decisions based on the type of the SchemaObject use an
  // enum and we also have generics for strong type checking
  public enum SchemaObjectType {
    COLLECTION(Constants.COLLECTION),
    DATABASE(Constants.DATABASE),
    INDEX(Constants.INDEX),
    KEYSPACE(Constants.KEYSPACE),
    TABLE(Constants.TABLE);

    /** Constants so the public HTTP API objects can use the same values. */
    public interface Constants {
      String COLLECTION = "Collection";
      String DATABASE = "Database";
      String INDEX = "Index";
      String KEYSPACE = "Keyspace";
      String TABLE = "Table";
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

  protected final SchemaObjectType type;
  protected final SchemaObjectName name;

  protected SchemaObject(SchemaObjectType type, SchemaObjectName name) {
    this.type = type;
    this.name = name;
  }

  public SchemaObjectType type() {
    return type;
  }

  public SchemaObjectName name() {
    return name;
  }

  /**
   * Subclasses must always return VectorConfig, if there is no vector config they should return
   * VectorConfig.notEnabledVectorConfig().
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

  @Override
  public String toString() {
    return toString(false);
  }

  @Override
  public PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
    return prettyToStringBuilder
        .append("type", type)
        .append("name.keyspace", name.keyspace())
        .append("name.table", name.table())
        .append("vectorConfig", vectorConfig())
        .append("indexUsage", newIndexUsage());
  }
}
