package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import java.util.Objects;

/** Builds a {@link DropIndexAttempt}. */
public class DropIndexAttemptBuilder {
  private int position;
  private final KeyspaceSchemaObject schemaObject;
  private final CQLOptions.BuildableCQLOptions<Drop> cqlOptions =
      new CQLOptions.BuildableCQLOptions<>();

  private CqlIdentifier indexName;
  private SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy;
  // must be specified, the default should not be defined in here
  private Boolean ifExists = null;

  public DropIndexAttemptBuilder(KeyspaceSchemaObject schemaObject) {
    this.schemaObject = Objects.requireNonNull(schemaObject, "schemaObject must not be null");
  }

  public DropIndexAttemptBuilder withIndexName(CqlIdentifier indexName) {
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    return this;
  }

  public DropIndexAttemptBuilder withSchemaRetryPolicy(
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy =
        Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");
    return this;
  }

  public DropIndexAttemptBuilder withIfExists(boolean ifExists) {
    this.ifExists = ifExists;
    return this;
  }

  public DropIndexAttempt build() {

    Objects.requireNonNull(indexName, "indexName must not be null");
    Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");
    Objects.requireNonNull(ifExists, "ifExists cannot be null");

    if (ifExists) {
      cqlOptions.addBuilderOption(CQLOption.ForDrop.ifExists());
    }
    return new DropIndexAttempt(position++, schemaObject, indexName, cqlOptions, schemaRetryPolicy);
  }
}
