package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.column.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand.Definition.Partitioning.OrderingKey;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand.Definition.Partitioning.OrderingKey.Order;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.SchemaChangeResult;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class CreateTableOperation implements Operation {

  private final CommandContext<KeyspaceSchemaObject> commandContext;
  private final String tableName;
  private final Map<String, ColumnType> columnTypes;
  private final List<String> partitionKeys;
  private final List<OrderingKey> clusteringKeys;
  private final String comment;

  public CreateTableOperation(
      CommandContext<KeyspaceSchemaObject> commandContext,
      String tableName,
      Map<String, ColumnType> columnTypes,
      List<String> partitionKeys,
      List<OrderingKey> clusteringKeys,
      String comment) {
    this.commandContext = commandContext;
    this.tableName = tableName;
    this.columnTypes = columnTypes;
    this.partitionKeys = partitionKeys;
    this.clusteringKeys = clusteringKeys;
    this.comment = comment;
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    CreateTableStart create =
        createTable(commandContext.schemaObject().name.keyspace(), tableName).ifNotExists();
    CreateTable createTable = addColumnsAndKeys(create, partitionKeys, clusteringKeys, columnTypes);
    CreateTableWithOptions createWithOptions = createTable.withComment(comment);
    createWithOptions = addClusteringOrder(createWithOptions, clusteringKeys);
    final SimpleStatement statement = createWithOptions.build();
    final Uni<AsyncResultSet> resultSetUni =
        queryExecutor.executeCreateSchemaChange(dataApiRequestInfo, statement);
    return resultSetUni.onItem().transform(rs -> new SchemaChangeResult(true));
  }

  private CreateTable addColumnsAndKeys(
      CreateTableStart create,
      List<String> partitionKeys,
      List<OrderingKey> clusteringKeys,
      Map<String, ColumnType> columnTypes) {
    CreateTable createTable = null;
    for (String partitionKey : partitionKeys) {
      if (createTable == null) {
        createTable =
            create.withPartitionKey(partitionKey, columnTypes.get(partitionKey).getCqlType());
      } else {
        createTable.withPartitionKey(partitionKey, columnTypes.get(partitionKey).getCqlType());
      }
      columnTypes.remove(partitionKey);
    }
    for (OrderingKey clusteringKey : clusteringKeys) {
      ColumnType columnType = columnTypes.get(clusteringKey.column());
      createTable =
          createTable.withClusteringColumn(clusteringKey.column(), columnType.getCqlType());
      columnTypes.remove(clusteringKey.column());
    }

    for (Map.Entry<String, ColumnType> column : columnTypes.entrySet()) {
      createTable = createTable.withColumn(column.getKey(), column.getValue().getCqlType());
    }
    return createTable;
  }

  private CreateTableWithOptions addClusteringOrder(
      CreateTableWithOptions createTableWithOptions, List<OrderingKey> clusteringKeys) {
    for (OrderingKey clusteringKey : clusteringKeys) {
      createTableWithOptions =
          createTableWithOptions.withClusteringOrder(
              clusteringKey.column(), getCqlClusterOrder(clusteringKey.order()));
    }
    return createTableWithOptions;
  }

  public ClusteringOrder getCqlClusterOrder(Order ordering) {
    return switch (ordering) {
      case ASC -> ClusteringOrder.ASC;
      case DESC -> ClusteringOrder.DESC;
    };
  }
}
