package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.operation.*;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTableOperation implements Operation {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableOperation.class);

  private final CommandContext<KeyspaceSchemaObject> commandContext;

  private final DriverExceptionHandler<KeyspaceSchemaObject> driverExceptionHandler;
  private final List<SchemaAttempt<KeyspaceSchemaObject>> attempts;
  private final OperationAttemptPageBuilder<
          KeyspaceSchemaObject, SchemaAttempt<KeyspaceSchemaObject>>
      pageBuilder;

  public CreateTableOperation(
      CommandContext<KeyspaceSchemaObject> commandContext,
      DriverExceptionHandler<KeyspaceSchemaObject> driverExceptionHandler,
      List<? extends SchemaAttempt<KeyspaceSchemaObject>> attempts,
      OperationAttemptPageBuilder<KeyspaceSchemaObject, SchemaAttempt<KeyspaceSchemaObject>>
          pageBuilder) {

    this.commandContext = commandContext;
    this.driverExceptionHandler = driverExceptionHandler;
    this.attempts = List.copyOf(attempts);
    this.pageBuilder = pageBuilder;
  }

  //  private final String tableName;
  //  private final Map<String, ColumnType> columnTypes;
  //  private final List<String> partitionKeys;
  //  private final List<PrimaryKey.OrderingKey> clusteringKeys;
  //  private final String comment;

  //  public CreateTableOperation(
  //      CommandContext<TableSchemaObject> commandContext,
  //      DriverExceptionHandler<SchemaT> driverExceptionHandler,
  //      List<? extends ReadAttempt<SchemaT>> readAttempts,
  //      ReadAttemptPage.Builder<SchemaT> pageBuilder) {
  //    this.commandContext = commandContext;
  //    this.tableName = tableName;
  //    this.columnTypes = Objects.requireNonNull(columnTypes, "columnTypes must not be null");
  //    this.partitionKeys = partitionKeys;
  //    this.clusteringKeys = clusteringKeys;
  //    this.comment = comment;
  //  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    // TODO AARON - for now we create the CommandQueryExecutor here , later change the Operation
    // interface
    CommandQueryExecutor commandQueryExecutor =
        new CommandQueryExecutor(
            queryExecutor.getCqlSessionCache(),
            new RequestContext(
                dataApiRequestInfo.getTenantId(), dataApiRequestInfo.getCassandraToken()),
            CommandQueryExecutor.QueryTarget.TABLE);

    return Multi.createFrom()
        .iterable(attempts)
        .onItem()
        .transformToUniAndConcatenate(
            attempt -> attempt.execute(commandQueryExecutor, driverExceptionHandler))
        .onItem()
        .transform(OperationAttempt::setSkippedIfReady)
        .collect()
        .in(() -> pageBuilder, OperationAttemptAccumulator::accumulate)
        .onItem()
        .transform(OperationAttemptPageBuilder::getOperationPage);
  }
  //
  //  private CreateTable addColumnsAndKeys(CreateTableStart create) {
  //    Set<String> addedColumns = new HashSet<>();
  //    CreateTable createTable = null;
  //    for (String partitionKey : partitionKeys) {
  //      if (createTable == null) {
  //        createTable =
  //            create.withPartitionKey(
  //                CqlIdentifier.fromInternal(partitionKey),
  //                columnTypes.get(partitionKey).getCqlType());
  //      } else {
  //        createTable =
  //            createTable.withPartitionKey(
  //                CqlIdentifier.fromInternal(partitionKey),
  //                columnTypes.get(partitionKey).getCqlType());
  //      }
  //      addedColumns.add(partitionKey);
  //    }
  //    for (PrimaryKey.OrderingKey clusteringKey : clusteringKeys) {
  //      ColumnType columnType = columnTypes.get(clusteringKey.column());
  //      createTable =
  //          createTable.withClusteringColumn(
  //              CqlIdentifier.fromInternal(clusteringKey.column()), columnType.getCqlType());
  //      addedColumns.add(clusteringKey.column());
  //    }
  //
  //    for (Map.Entry<String, ColumnType> column : columnTypes.entrySet()) {
  //      if (addedColumns.contains(column.getKey())) {
  //        continue;
  //      }
  //      createTable =
  //          createTable.withColumn(
  //              CqlIdentifier.fromInternal(column.getKey()), column.getValue().getCqlType());
  //    }
  //    return createTable;
  //  }
  //
  //  private CreateTableWithOptions addClusteringOrder(CreateTableWithOptions
  // createTableWithOptions) {
  //    for (PrimaryKey.OrderingKey clusteringKey : clusteringKeys) {
  //      createTableWithOptions =
  //          createTableWithOptions.withClusteringOrder(
  //              clusteringKey.column(), getCqlClusterOrder(clusteringKey.order()));
  //    }
  //    return createTableWithOptions;
  //  }
  //
  //  public ClusteringOrder getCqlClusterOrder(PrimaryKey.OrderingKey.Order ordering) {
  //    return switch (ordering) {
  //      case ASC -> ClusteringOrder.ASC;
  //      case DESC -> ClusteringOrder.DESC;
  //    };
  //  }
}
