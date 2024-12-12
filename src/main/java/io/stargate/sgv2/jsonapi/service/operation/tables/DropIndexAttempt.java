package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import java.util.Objects;

/*
 An attempt to drop index for a column.
*/
public class DropIndexAttempt extends SchemaAttempt<KeyspaceSchemaObject> {
  private final CqlIdentifier indexName;
  private final CQLOptions<Drop> cqlOptions;

  public DropIndexAttempt(
      int position,
      KeyspaceSchemaObject schemaObject,
      CqlIdentifier indexName,
      CQLOptions<Drop> cqlOptions,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    super(position, schemaObject, schemaRetryPolicy);

    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    this.cqlOptions = Objects.requireNonNull(cqlOptions, "cqlOptions must not be null");
    setStatus(OperationStatus.READY);
  }

  @Override
  protected SimpleStatement buildStatement() {

    // The keyspace name always comes from the metadata, we need to add support to hold the
    // KeyspaceMetadata
    Drop drop =
        SchemaBuilder.dropIndex(
            CqlIdentifier.fromInternal(schemaObject.name().keyspace()), indexName);

    // Apply any additional options
    drop = cqlOptions.applyBuilderOptions(drop);

    return drop.build();
  }
}
