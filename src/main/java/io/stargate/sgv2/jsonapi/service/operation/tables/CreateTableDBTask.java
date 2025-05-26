package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableExtensions;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import java.util.Map;
import java.util.Objects;

public class CreateTableDBTask extends SchemaDBTask<KeyspaceSchemaObject> {

  private final ApiTableDef tableDef;
  private final Map<String, String> customProperties;
  private final boolean ifNotExists;

  // TODO: THIS MUST BE GIVEN STATEMENT BUILDERS LIKE THE OTHER ATTEMPTS NOT PASSED IN API OBJECTS
  protected CreateTableDBTask(
      int position,
      KeyspaceSchemaObject schemaObject,
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy,
      DefaultDriverExceptionHandler.Factory<KeyspaceSchemaObject> exceptionHandlerFactory,
      ApiTableDef tableDef,
      boolean ifNotExists,
      Map<String, String> customProperties) {
    super(position, schemaObject, schemaRetryPolicy, exceptionHandlerFactory);

    this.tableDef = Objects.requireNonNull(tableDef, "tableDef must not be null");
    this.ifNotExists = ifNotExists;
    this.customProperties = customProperties;

    setStatus(TaskStatus.READY);
  }

  public static CreateTableDBTaskBuilder builder(KeyspaceSchemaObject schemaObject) {
    return new CreateTableDBTaskBuilder(schemaObject);
  }

  protected SimpleStatement buildStatement() {

    CreateTableStart create = createTable(schemaObject.identifier().keyspace(), tableDef.name());

    // Add if not exists flag based on request
    if (ifNotExists) {
      create = create.ifNotExists();
    }
    // Add all primary keys and columns
    CreateTable createTable = addColumnsAndKeys(create);

    var extensions = TableExtensions.toExtensions(customProperties);
    CreateTableWithOptions createWithOptions =
        createTable.withOption(
            TableExtensions.TABLE_OPTIONS_EXTENSION_KEY.asInternal(), extensions);

    // Add the clustering key order
    createWithOptions = addClusteringOrder(createWithOptions);

    return createWithOptions.build();
  }

  private CreateTable addColumnsAndKeys(CreateTableStart createTableStart) {

    CreateTable createTable = null;
    for (var partitionDef : tableDef.partitionKeys().values()) {

      createTable =
          (createTable == null)
              ? createTableStart.withPartitionKey(
                  partitionDef.name(), partitionDef.type().cqlType())
              : createTable.withPartitionKey(partitionDef.name(), partitionDef.type().cqlType());
    }

    for (var clusteringDef : tableDef.clusteringDefs()) {
      createTable =
          createTable.withClusteringColumn(
              clusteringDef.columnDef().name(), clusteringDef.columnDef().type().cqlType());
    }

    for (var columnDef : tableDef.nonPKColumns().values()) {
      createTable = createTable.withColumn(columnDef.name(), columnDef.type().cqlType());
    }
    return createTable;
  }

  private CreateTableWithOptions addClusteringOrder(CreateTableWithOptions createTableWithOptions) {

    for (var clusteringDef : tableDef.clusteringDefs()) {
      createTableWithOptions =
          createTableWithOptions.withClusteringOrder(
              clusteringDef.columnDef().name(), clusteringDef.order().getCqlOrder());
    }

    return createTableWithOptions;
  }
}
