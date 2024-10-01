package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import java.time.Duration;
import java.util.*;

public class CreateTableAttempt extends SchemaAttempt<KeyspaceSchemaObject> {

  private final String tableName;
  private final Map<String, ColumnType> columnTypes;
  private final List<String> partitionKeys;
  private final List<PrimaryKey.OrderingKey> clusteringKeys;
  private final String comment;

  public CreateTableAttempt(
      int position,
      KeyspaceSchemaObject schemaObject,
      int retryDelayMillis,
      int maxRetries,
      String tableName,
      Map<String, ColumnType> columnTypes,
      List<String> partitionKeys,
      List<PrimaryKey.OrderingKey> clusteringKeys,
      String comment) {
    super(
        position,
        schemaObject,
        new SchemaRetryPolicy(maxRetries, Duration.ofMillis(retryDelayMillis)));

    this.tableName = tableName;
    this.columnTypes = Objects.requireNonNull(columnTypes, "columnTypes must not be null");
    this.partitionKeys = partitionKeys;
    this.clusteringKeys = clusteringKeys;
    this.comment = comment;

    setStatus(OperationStatus.READY);
  }

  protected SimpleStatement buildStatement() {

    var keyspaceIdentifier = cqlIdentifierFromUserInput(schemaObject.name().keyspace());
    var tableIdentifier = cqlIdentifierFromUserInput(tableName);

    CreateTableStart create = createTable(keyspaceIdentifier, tableIdentifier).ifNotExists();

    // Add all primary keys and colunms
    CreateTable createTable = addColumnsAndKeys(create);

    // Add comment which has table properties for vectorize
    CreateTableWithOptions createWithOptions = createTable.withComment(comment);

    // Add the clustering key order
    createWithOptions = addClusteringOrder(createWithOptions);

    return createWithOptions.build();
  }

  private CreateTable addColumnsAndKeys(CreateTableStart create) {
    Set<String> addedColumns = new HashSet<>();
    CreateTable createTable = null;
    for (String partitionKey : partitionKeys) {
      if (createTable == null) {
        createTable =
            create.withPartitionKey(
                CqlIdentifier.fromInternal(partitionKey),
                columnTypes.get(partitionKey).getCqlType());
      } else {
        createTable =
            createTable.withPartitionKey(
                CqlIdentifier.fromInternal(partitionKey),
                columnTypes.get(partitionKey).getCqlType());
      }
      addedColumns.add(partitionKey);
    }
    for (PrimaryKey.OrderingKey clusteringKey : clusteringKeys) {
      ColumnType columnType = columnTypes.get(clusteringKey.column());
      createTable =
          createTable.withClusteringColumn(
              CqlIdentifier.fromInternal(clusteringKey.column()), columnType.getCqlType());
      addedColumns.add(clusteringKey.column());
    }

    for (Map.Entry<String, ColumnType> column : columnTypes.entrySet()) {
      if (addedColumns.contains(column.getKey())) {
        continue;
      }
      createTable =
          createTable.withColumn(
              CqlIdentifier.fromInternal(column.getKey()), column.getValue().getCqlType());
    }
    return createTable;
  }

  private CreateTableWithOptions addClusteringOrder(CreateTableWithOptions createTableWithOptions) {
    for (PrimaryKey.OrderingKey clusteringKey : clusteringKeys) {
      createTableWithOptions =
          createTableWithOptions.withClusteringOrder(
              clusteringKey.column(), getCqlClusterOrder(clusteringKey.order()));
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
