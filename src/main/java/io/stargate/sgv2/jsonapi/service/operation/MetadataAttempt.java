package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.EmptyAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** An attempt to execute commands that need data from metadata */
public abstract class MetadataAttempt<SchemaT extends SchemaObject>
    extends OperationAttempt<MetadataAttempt<SchemaT>, SchemaT> {
  // this will be set on executeStatement
  // TODO: BETTER CONTROL ON WHEN THIS IS SET AND NOT SET
  protected Optional<KeyspaceMetadata> keyspaceMetadata;

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

  protected abstract List<String> getNames();

  protected abstract Object getSchema();

  @Override
  protected Uni<AsyncResultSet> executeStatement(CommandQueryExecutor queryExecutor) {

    this.keyspaceMetadata = queryExecutor.getKeyspaceMetadata(schemaObject.name().keyspace());

    if (keyspaceMetadata.isEmpty()) {
      return Uni.createFrom()
          .failure(
              SchemaException.Code.INVALID_KEYSPACE.get(
                  Map.of("keyspace", schemaObject.name().keyspace())));
    }
    return Uni.createFrom().item(new EmptyAsyncResultSet());
  }

  /**
   * Convert table schema object to table response which is returned as response for `listTables`
   *
   * @return
   */
  //  protected TableDesc getTableSchema(TableSchemaObject tableSchemaObject) {
  //
  //    var tableMetadata = tableSchemaObject.tableMetadata();
  //
  //    var apiColumnDefs = new ListTablesAttempt(tableMetadata.getColumns())
  //
  //    for (ColumnMetadata columnMetadata : tableMetadata.getColumns().values()) {
  //      // convert the ColumnMetadata to ApiColumnDef, then from there into the ColumnType
  //      try {
  //        tableColumns.put(
  //            columnMetadata.getName(),
  //            getApiDataType(columnMetadata, tableSchemaObject.vectorConfig()).getColumnType());
  //      } catch (UnsupportedCqlType e) {
  //        tableColumns.put(
  //            columnMetadata.getName(),
  //            new ComplexColumnType.UnsupportedType(columnMetadata.getType()));
  //      }
  //    }
  //
  //    var partitionSort =
  //        tableMetadata.getClusteringColumns().entrySet().stream()
  //            .map(entry -> PrimaryKey.OrderingKey.from(entry.getKey().getName(),
  // entry.getValue()))
  //            .toList();
  //    var partitionBy =
  //        tableMetadata.getPartitionKey().stream().map(ColumnMetadata::getName).toList();
  //    var primaryKey = PrimaryKey.from(partitionBy, partitionSort);
  //
  //    return new TableDesc(
  //        tableMetadata.getName(), new TableDefinition(tableColumns, primaryKey));
  //  }

  //  private ApiDataType getApiDataType(ColumnMetadata columnMetadata, VectorConfig vectorConfig)
  //      throws UnsupportedCqlType {
  //
  //    if (columnMetadata.getType() instanceof VectorType vt) {
  //      // Special handling because we need to get the vectorize config from the vector config
  //
  //      // TODO: WHAT HAPPENS IF THIS IS NOT PRESENT ???
  //      // TODO: HACK AARON - remove call is asInternal
  //      var columnVectorDefinition =
  //          vectorConfig.columnVectorDefinitions().stream()
  //              .filter(vc -> vc.fieldName().equals(columnMetadata.getName().asInternal()))
  //              .findFirst()
  //              .get();
  //
  //      return ComplexApiDataType.ApiVectorType.from(
  //          ApiDataTypeDefs.from(vt.getElementType()),
  //          vt.getDimensions(),
  //          columnVectorDefinition.vectorizeDefinition());
  //
  //    } else {
  //      return ApiDataTypeDefs.from(columnMetadata.getType());
  //    }
  //  }

  //  @JsonPropertyOrder({"columns", "primaryKey"})
  //  @JsonInclude(JsonInclude.Include.NON_NULL)
  //  record TableDefinition(
  //      @JsonIgnore Map<CqlIdentifier, ColumnType> tableColumns, PrimaryKey primaryKey) {
  //
  //    public Map<String, ColumnType> getColumns() {
  //      return tableColumns.entrySet().stream()
  //          .collect(
  //              Collectors.toMap(
  //                  entry -> cqlIdentifierToJsonKey(entry.getKey()), Map.Entry::getValue));
  //    }
  //  }

}
