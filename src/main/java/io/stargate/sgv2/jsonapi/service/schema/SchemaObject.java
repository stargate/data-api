package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.IndexUsage;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;

/** A Collection or Table the command works on */
public abstract class SchemaObject implements Recordable {

  protected final SchemaObjectIdentifier identifier;

  protected SchemaObject(SchemaObjectType expectedType, SchemaObjectIdentifier identifier) {

    this.identifier = Objects.requireNonNull(identifier, "identifier must not be null");

    if (identifier.type() != expectedType) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid SchemaObjectIdentifier, expected schemaType %s but got identifier: %s", expectedType, identifier));
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
  public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    return dataRecorder.append("identifier", identifier);
  }
}
