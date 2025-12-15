package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.IndexUsage;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;

/**
 * Base for all Schema objects the API works with, such as Database, Keyspace, Table, Collection.
 *
 * <p>Schema Object are identified by a {@link SchemaObjectIdentifier} which is globally unique for
 * all tenants.
 */
public abstract class SchemaObject implements Recordable {

  protected final SchemaObjectIdentifier identifier;

  protected SchemaObject(SchemaObjectType expectedType, SchemaObjectIdentifier identifier) {

    this.identifier = Objects.requireNonNull(identifier, "identifier must not be null");

    if (identifier.type() != expectedType) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid SchemaObjectIdentifier, expected schemaType %s but got identifier: %s",
              expectedType, identifier));
    }
  }

  public SchemaObjectType type() {
    return identifier.type();
  }

  public SchemaObjectIdentifier identifier() {
    return identifier;
  }

  /**
   * Subclasses must always return VectorConfig, if there is no vector config they should return
   * VectorConfig.notEnabledVectorConfig().
   *
   * <p>aaron - 30 may 2025 - this is legacy from old code and should be moved in the future.
   */
  public abstract VectorConfig vectorConfig();

  /**
   * Call to get an instance of the appropriate {@link IndexUsage} for this schema object
   *
   * <p>aaron - 30 may 2025 - this is legacy from old code and should be moved in the future.
   */
  public abstract IndexUsage newIndexUsage();

  @Override
  public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    return dataRecorder.append("identifier", identifier);
  }
}
