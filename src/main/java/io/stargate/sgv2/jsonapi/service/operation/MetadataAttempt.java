package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexTypes;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveTypes;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableMatcher;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public abstract class MetadataAttempt<SchemaT extends KeyspaceSchemaObject>
        extends OperationAttempt<MetadataAttempt<SchemaT>, SchemaT>{
  // this will be set on executeStatement
  private Optional<KeyspaceMetadata> keyspaceMetadata;
  private static ObjectMapper objectMapper = new ObjectMapper();
  private static final CollectionTableMatcher TABLE_MATCHER = new CollectionTableMatcher();

  /**
   * Create a new {@link OperationAttempt} with the provided position, schema object and retry
   * policy.
   *
   * @param position The 0 based position of the attempt in the container of attempts. Attempts are
   *     ordered by position, for sequential processing and for rebuilding the response in the
   *     correct order (e.g. for inserting many documents)
   * @param schemaObject The schema object that the operation is working with.
   * @param retryPolicy The {@link RetryPolicy} to use when running the operation, if there is no
   *     retry policy then use {@link RetryPolicy#NO_RETRY}
   */
  protected MetadataAttempt(int position, SchemaT schemaObject, RetryPolicy retryPolicy) {
    super(position, schemaObject, retryPolicy);
  }

  @Override
  protected Uni<AsyncResultSet> executeStatement(CommandQueryExecutor queryExecutor) {
    this.keyspaceMetadata = queryExecutor.getKeyspaceMetadata(schemaObject.name().keyspace());
    return Uni.createFrom().item(new EmptyAsyncResultSet());
  }

  /**
   * Convert table schema object to table response which is returned as response for `listTables`
   *
   * @return
   */
  protected TableResponse getTableSchema(TableSchemaObject tableSchemaObject) {
    TableMetadata tableMetadata = tableSchemaObject.tableMetadata();
    String tableName = CqlIdentifierUtil.externalRepresentation(tableMetadata.getName());
    HashMap<String, ColumnType> columnsDefinition = new HashMap<>();
    for (Map.Entry<CqlIdentifier, ColumnMetadata> column : tableMetadata.getColumns().entrySet()) {
      ColumnType type =
          getColumnType(
              column.getKey().asInternal(), column.getValue(), tableSchemaObject.vectorConfig());
      columnsDefinition.put(CqlIdentifierUtil.externalRepresentation(column.getKey()), type);
    }

    final List<String> partitionBy =
        tableMetadata.getPartitionKey().stream()
            .map(column -> CqlIdentifierUtil.externalRepresentation(column.getName()))
            .collect(Collectors.toList());
    final List<PrimaryKey.OrderingKey> partitionSort =
        tableMetadata.getClusteringColumns().entrySet().stream()
            .map(
                entry ->
                    new PrimaryKey.OrderingKey(
                        CqlIdentifierUtil.externalRepresentation(entry.getKey().getName()),
                        entry.getValue() == ClusteringOrder.ASC
                            ? PrimaryKey.OrderingKey.Order.ASC
                            : PrimaryKey.OrderingKey.Order.DESC))
            .collect(Collectors.toList());
    PrimaryKey primaryKey =
        new PrimaryKey(
            partitionBy.toArray(new String[0]),
            partitionSort.toArray(new PrimaryKey.OrderingKey[0]));
    return new TableResponse(
        tableName, new TableResponse.TableDefinition(columnsDefinition, primaryKey));
  }

  private ColumnType getColumnType(
      String columnName, ColumnMetadata columnMetadata, VectorConfig vectorConfig) {
    if (columnMetadata.getType() instanceof VectorType vt) {
      // Schema will always have VectorConfig for vector type
      VectorConfig.ColumnVectorDefinition columnVectorDefinition =
          vectorConfig.columnVectorDefinitions().stream()
              .filter(vc -> vc.fieldName().equals(columnName))
              .findFirst()
              .get();
      VectorizeConfig vectorizeConfig =
          columnVectorDefinition.vectorizeConfig() == null
              ? null
              : new VectorizeConfig(
                  columnVectorDefinition.vectorizeConfig().provider(),
                  columnVectorDefinition.vectorizeConfig().modelName(),
                  columnVectorDefinition.vectorizeConfig().authentication(),
                  columnVectorDefinition.vectorizeConfig().parameters());
      return new ComplexTypes.VectorType(PrimitiveTypes.FLOAT, vt.getDimensions(), vectorizeConfig);
    } else if (columnMetadata.getType() instanceof MapType mt) {
      if (!mt.isFrozen()) {
        final Optional<ApiDataTypeDef> apiDataTypeDefKey = ApiDataTypeDefs.from(mt.getKeyType());
        final Optional<ApiDataTypeDef> apiDataTypeDefValue =
            ApiDataTypeDefs.from(mt.getValueType());
        if (apiDataTypeDefKey.isPresent() && apiDataTypeDefValue.isPresent()) {
          return new ComplexTypes.MapType(
              PrimitiveTypes.fromString(apiDataTypeDefKey.get().getApiType().getApiName()),
              PrimitiveTypes.fromString(apiDataTypeDefValue.get().getApiType().getApiName()));
        }
      }
      // return unsupported format
      return new ComplexTypes.UnsupportedType(mt.asCql(true, false));

    } else if (columnMetadata.getType()
        instanceof com.datastax.oss.driver.api.core.type.ListType lt) {
      if (!lt.isFrozen()) {
        final Optional<ApiDataTypeDef> apiDataTypeDef = ApiDataTypeDefs.from(lt.getElementType());
        if (apiDataTypeDef.isPresent()) {
          return new ComplexTypes.ListType(
              PrimitiveTypes.fromString(apiDataTypeDef.get().getApiType().getApiName()));
        }
      }
      // return unsupported format
      return new ComplexTypes.UnsupportedType(lt.asCql(true, false));

    } else if (columnMetadata.getType()
        instanceof com.datastax.oss.driver.api.core.type.SetType st) {
      if (!st.isFrozen()) {
        final Optional<ApiDataTypeDef> apiDataTypeDef = ApiDataTypeDefs.from(st.getElementType());
        if (apiDataTypeDef.isPresent()) {
          return new ComplexTypes.SetType(
              PrimitiveTypes.fromString(apiDataTypeDef.get().getApiType().getApiName()));
        }
      }
      // return unsupported format
      return new ComplexTypes.UnsupportedType(st.asCql(true, false));
    } else {
      final Optional<ApiDataTypeDef> apiDataTypeDef =
          ApiDataTypeDefs.from(columnMetadata.getType());
      if (apiDataTypeDef.isPresent())
        return PrimitiveTypes.fromString(apiDataTypeDef.get().getApiType().getApiName());
      else {
        // Need to return unsupported type
        return new ComplexTypes.UnsupportedType(columnMetadata.getType().asCql(true, false));
      }
    }
  }

  /**
   * Object used to build the response for listTables command
   *
   * @param name
   * @param definition
   */
  @JsonPropertyOrder({"name", "definition"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record TableResponse(String name, TableDefinition definition) {

    @JsonPropertyOrder({"columns", "primaryKey"})
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record TableDefinition(Map<String, ColumnType> columns, PrimaryKey primaryKey) {}
  }

  protected List<TableSchemaObject> getTables() {
    return keyspaceMetadata
        .get()
        // get all tables
        .getTables()
        .values()
        .stream()
        // filter for valid collections
        .filter(TABLE_MATCHER.negate())
        // map to name
        .map(table -> TableSchemaObject.from(table, objectMapper))
        // get as list
        .toList();
  }

  /**
   * EmptyAsyncResultSet implementation to be used only for metadata attempt where no cal query is
   * run.
   */
  private static class EmptyAsyncResultSet implements AsyncResultSet {
    @NonNull
    @Override
    public ColumnDefinitions getColumnDefinitions() {
      return EmptyColumnDefinitions.INSTANCE;
    }

    @NonNull
    @Override
    public ExecutionInfo getExecutionInfo() {
      return null;
    }

    @NonNull
    @Override
    public Iterable<Row> currentPage() {
      return Collections.emptyList();
    }

    @Override
    public int remaining() {
      return 0;
    }

    @Override
    public boolean hasMorePages() {
      return false;
    }

    @NonNull
    @Override
    public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
      throw new IllegalStateException(
          "No next page. Use #hasMorePages before calling this method to avoid this error.");
    }

    @Override
    public boolean wasApplied() {
      return true;
    }
  }
}
