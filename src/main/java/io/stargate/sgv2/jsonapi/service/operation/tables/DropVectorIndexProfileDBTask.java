package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.alterTable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableExtensions;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import java.util.Map;
import java.util.Objects;

/**
 * Removes a dropped index's entry from its owning table's vector-index-profiles extension, so the
 * profile record does not outlive the index.
 *
 * <p>Keyspace-scoped so it can share a {@link
 * io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup} with {@link DropIndexDBTask}; a
 * {@link io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject}-typed {@link
 * AlterTableDBTask} (the create side) cannot, since a TaskGroup has a single schema-object type.
 * Owning table and rewritten extensions payload are resolved at command-resolve time via {@link
 * TableExtensions#removeIndexProfile}; this task only issues the {@code ALTER TABLE ... WITH
 * extensions = {...}}.
 */
public class DropVectorIndexProfileDBTask extends SchemaDBTask<KeyspaceSchemaObject> {

  private final CqlIdentifier tableName;
  private final Map<String, String> customProperties;

  public DropVectorIndexProfileDBTask(
      int position,
      KeyspaceSchemaObject schemaObject,
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy,
      DefaultDriverExceptionHandler.Factory<KeyspaceSchemaObject> exceptionHandlerFactory,
      CqlIdentifier tableName,
      Map<String, String> customProperties) {
    super(position, schemaObject, schemaRetryPolicy, exceptionHandlerFactory);

    this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
    this.customProperties =
        Objects.requireNonNull(customProperties, "customProperties must not be null");
    setStatus(TaskStatus.READY);
  }

  public static DropVectorIndexProfileDBTaskBuilder builder(KeyspaceSchemaObject schemaObject) {
    return new DropVectorIndexProfileDBTaskBuilder(schemaObject);
  }

  @Override
  protected SimpleStatement buildStatement() {

    // owning table lives in this keyspace; keyspace from the schema object identifier, as
    // DropIndexDBTask does
    var extensions = TableExtensions.toExtensions(customProperties);
    return alterTable(schemaObject.identifier().keyspace(), tableName)
        .withOption(TableExtensions.TABLE_OPTIONS_EXTENSION_KEY.asInternal(), extensions)
        .build();
  }
}
