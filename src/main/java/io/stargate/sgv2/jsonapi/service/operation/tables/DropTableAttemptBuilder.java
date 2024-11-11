package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;

/** Builds a {@link DropTableAttempt}. */
public class DropTableAttemptBuilder {
  private int position;
  private final KeyspaceSchemaObject schemaObject;
  private final CqlIdentifier name;
  private final SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy;
  private final CQLOptions<Drop> cqlOptions = new CQLOptions<>();

  public DropTableAttemptBuilder(
      KeyspaceSchemaObject schemaObject,
      String tableName,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaObject = schemaObject;
    this.schemaRetryPolicy = schemaRetryPolicy;
    this.name = CqlIdentifierUtil.cqlIdentifierFromUserInput(tableName);
  }

  public DropTableAttemptBuilder withIfExists(CQLOption<Drop> cqlOption) {
    if (cqlOption != null) {
      this.cqlOptions.addBuilderOption(cqlOption);
    }
    return this;
  }

  public DropTableAttempt build() {
    return new DropTableAttempt(position++, schemaObject, name, cqlOptions, schemaRetryPolicy);
  }
}
