package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.SchemaChangeResult;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import io.stargate.sgv2.jsonapi.service.schema.tables.ComplexApiDataType;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTableOperation implements Operation {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableOperation.class);

  private final CommandContext<KeyspaceSchemaObject> commandContext;
  private final String tableName;
  private final Map<String, ApiDataType> columnTypes;
  private final List<String> partitionKeys;
  private final List<PrimaryKey.OrderingKey> clusteringKeys;
  private final String comment;
  private final boolean ifNotExists;

  public CreateTableOperation(
      CommandContext<KeyspaceSchemaObject> commandContext,
      String tableName,
      Map<String, ApiDataType> columnTypes,
      List<String> partitionKeys,
      List<PrimaryKey.OrderingKey> clusteringKeys,
      boolean ifNotExists,
      String comment) {
    this.commandContext = commandContext;
    this.tableName = tableName;
    this.columnTypes = Objects.requireNonNull(columnTypes, "columnTypes must not be null");
    this.partitionKeys = partitionKeys;
    this.clusteringKeys = clusteringKeys;
    this.ifNotExists = ifNotExists;
    this.comment = comment;
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    CqlIdentifier keyspaceIdentifier =
        CqlIdentifier.fromInternal(commandContext.schemaObject().name().keyspace());
    CqlIdentifier tableIdentifier = CqlIdentifier.fromInternal(tableName);
    CreateTableStart create = createTable(keyspaceIdentifier, tableIdentifier);

    // Add if not exists flag based on request
    if (ifNotExists) {
      create = create.ifNotExists();
    }
    // Add all primary keys and colunms
    CreateTable createTable = addColumnsAndKeys(create);

    // Add comment which has table properties for vectorize
    CreateTableWithOptions createWithOptions = createTable.withComment(comment);

    // Add the clustering key order
    createWithOptions = addClusteringOrder(createWithOptions);

    final SimpleStatement statement = createWithOptions.build();
    LOGGER.warn("CREATE TABLE CQL: {}", createWithOptions.asCql());

    final Uni<AsyncResultSet> resultSetUni =
        queryExecutor.executeCreateSchemaChange(dataApiRequestInfo, statement);

    return resultSetUni.onItem().transform(rs -> new SchemaChangeResult(true));
  }

  private CreateTable addColumnsAndKeys(CreateTableStart create) {
    Set<String> addedColumns = new HashSet<>();
    CreateTable createTable = null;
    for (String partitionKey : partitionKeys) {
      DataType dataType = getCqlDataType(columnTypes.get(partitionKey));
      if (createTable == null) {
        createTable = create.withPartitionKey(CqlIdentifier.fromInternal(partitionKey), dataType);
      } else {
        createTable =
            createTable.withPartitionKey(CqlIdentifier.fromInternal(partitionKey), dataType);
      }
      addedColumns.add(partitionKey);
    }
    for (PrimaryKey.OrderingKey clusteringKey : clusteringKeys) {
      ApiDataType apiDataType = columnTypes.get(clusteringKey.column());
      DataType dataType = getCqlDataType(apiDataType);
      createTable =
          createTable.withClusteringColumn(
              CqlIdentifier.fromInternal(clusteringKey.column()), dataType);
      addedColumns.add(clusteringKey.column());
    }

    for (Map.Entry<String, ApiDataType> column : columnTypes.entrySet()) {
      if (addedColumns.contains(column.getKey())) {
        continue;
      }
      DataType dataType = getCqlDataType(column.getValue());
      createTable = createTable.withColumn(CqlIdentifier.fromInternal(column.getKey()), dataType);
    }
    return createTable;
  }

  private DataType getCqlDataType(ApiDataType apiDataType) {
    if (apiDataType instanceof ComplexApiDataType) {
      return ((ComplexApiDataType) apiDataType).getCqlType();
    } else {
      return ApiDataTypeDefs.from(apiDataType).get().getCqlType();
    }
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
