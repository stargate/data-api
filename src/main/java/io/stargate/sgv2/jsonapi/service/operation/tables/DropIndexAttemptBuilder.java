package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.CqlOptions;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;

/** Builds a {@link DropIndexAttempt}. */
public class DropIndexAttemptBuilder {
  private int position;
  private final KeyspaceSchemaObject schemaObject;
  private final CqlIdentifier name;
  private CqlOptions<Drop> cqlOptions = new CqlOptions<>();
  private final SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy;

  public DropIndexAttemptBuilder(
      KeyspaceSchemaObject schemaObject,
      String indexName,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaObject = schemaObject;
    this.schemaRetryPolicy = schemaRetryPolicy;
    this.name = CqlIdentifierUtil.cqlIdentifierFromUserInput(indexName);
  }

  public DropIndexAttemptBuilder withIfExists(CQLOption<Drop> cqlOption) {
    if (cqlOption != null) {
      this.cqlOptions.addBuilderOption(cqlOption);
    }
    return this;
  }

  public DropIndexAttempt build() {
    return new DropIndexAttempt(position++, schemaObject, name, cqlOptions, schemaRetryPolicy);
  }
}
