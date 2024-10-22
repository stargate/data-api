package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CreateTableAttempt extends SchemaAttempt<KeyspaceSchemaObject> {

  private final String tableName;
  private final Map<String, ApiDataType> columnTypes;
  private final List<String> partitionKeys;
  private final List<PrimaryKey.OrderingKey> clusteringKeys;
  private final Map<String, String> customProperties;
  private final boolean ifNotExists;

  protected CreateTableAttempt(
      int position,
      KeyspaceSchemaObject schemaObject,
      int retryDelayMillis,
      int maxRetries,
      String tableName,
      Map<String, ApiDataType> columnTypes,
      List<String> partitionKeys,
      List<PrimaryKey.OrderingKey> clusteringKeys,
      boolean ifNotExists,
      Map<String, String> customProperties) {
    super(
        position,
        schemaObject,
        new SchemaRetryPolicy(maxRetries, Duration.ofMillis(retryDelayMillis)));

    this.tableName = tableName;
    this.columnTypes = Objects.requireNonNull(columnTypes, "columnTypes must not be null");
    this.partitionKeys = partitionKeys;
    this.clusteringKeys = clusteringKeys;
    this.ifNotExists = ifNotExists;
    this.customProperties = customProperties;

    setStatus(OperationStatus.READY);
  }

  protected SimpleStatement buildStatement() {

    var keyspaceIdentifier = cqlIdentifierFromUserInput(schemaObject.name().keyspace());
    var tableIdentifier = cqlIdentifierFromUserInput(tableName);

    CreateTableStart create = createTable(keyspaceIdentifier, tableIdentifier);

    // Add if not exists flag based on request
    if (ifNotExists) {
      create = create.ifNotExists();
    }
    // Add all primary keys and colunms
    CreateTable createTable = addColumnsAndKeys(create);

    final Map<String, String> extensions = encodeAsHexValue(customProperties);

    CreateTableWithOptions createWithOptions = createTable.withOption("extensions", extensions);

    // Add the clustering key order
    createWithOptions = addClusteringOrder(createWithOptions);

    return createWithOptions.build();
  }

  private CreateTable addColumnsAndKeys(CreateTableStart create) {
    Set<String> addedColumns = new HashSet<>();
    CreateTable createTable = null;
    for (String partitionKey : partitionKeys) {
      DataType dataType = getCqlDataType(columnTypes.get(partitionKey));
      if (createTable == null) {
        createTable = create.withPartitionKey(cqlIdentifierFromUserInput(partitionKey), dataType);
      } else {
        createTable =
            createTable.withPartitionKey(cqlIdentifierFromUserInput(partitionKey), dataType);
      }
      addedColumns.add(partitionKey);
    }
    for (PrimaryKey.OrderingKey clusteringKey : clusteringKeys) {
      ApiDataType apiDataType = columnTypes.get(clusteringKey.column());
      DataType dataType = getCqlDataType(apiDataType);
      createTable =
          createTable.withClusteringColumn(
              cqlIdentifierFromUserInput(clusteringKey.column()), dataType);
      addedColumns.add(clusteringKey.column());
    }

    for (Map.Entry<String, ApiDataType> column : columnTypes.entrySet()) {
      if (addedColumns.contains(column.getKey())) {
        continue;
      }
      DataType dataType = getCqlDataType(column.getValue());
      createTable = createTable.withColumn(cqlIdentifierFromUserInput(column.getKey()), dataType);
    }
    return createTable;
  }

  private CreateTableWithOptions addClusteringOrder(CreateTableWithOptions createTableWithOptions) {
    for (PrimaryKey.OrderingKey clusteringKey : clusteringKeys) {
      createTableWithOptions =
          createTableWithOptions.withClusteringOrder(
              cqlIdentifierFromUserInput(clusteringKey.column()),
              getCqlClusterOrder(clusteringKey.order()));
    }
    return createTableWithOptions;
  }

  public ClusteringOrder getCqlClusterOrder(PrimaryKey.OrderingKey.Order ordering) {
    return switch (ordering) {
      case ASC -> ClusteringOrder.ASC;
      case DESC -> ClusteringOrder.DESC;
    };
  }
}
