package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import java.util.Objects;

/*
 An attempt to drop index for a column.
*/
public class DropIndexDBTask extends SchemaDBTask<KeyspaceSchemaObject> {

  private final CqlIdentifier indexName;
  private final CQLOptions<Drop> cqlOptions;

  public DropIndexDBTask(
      int position,
      KeyspaceSchemaObject schemaObject,
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy,
      DefaultDriverExceptionHandler.Factory<KeyspaceSchemaObject> exceptionHandlerFactory,
      CqlIdentifier indexName,
      CQLOptions<Drop> cqlOptions) {
    super(position, schemaObject, schemaRetryPolicy, exceptionHandlerFactory);

    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    this.cqlOptions = Objects.requireNonNull(cqlOptions, "cqlOptions must not be null");
    setStatus(TaskStatus.READY);
  }

  public static DropIndexDBTaskBuilder builder(KeyspaceSchemaObject schemaObject) {
    return new DropIndexDBTaskBuilder(schemaObject);
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
