package io.stargate.sgv2.jsonapi.service.operation;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexColumnType;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.EmptyAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableMatcher;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** An attempt to execute commands that need data from metadata */
public abstract class MetadataAttempt<SchemaT extends SchemaObject>
    extends OperationAttempt<MetadataAttempt<SchemaT>, SchemaT> {
  // this will be set on executeStatement
  private Optional<KeyspaceMetadata> keyspaceMetadata;

  private static final ObjectMapper objectMapper = new ObjectMapper();
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
  protected TableResponse getTableSchema(TableSchemaObject tableSchemaObject) {

    var tableMetadata = tableSchemaObject.tableMetadata();

    HashMap<CqlIdentifier, ColumnType> tableColumns =
        new HashMap<>(tableMetadata.getColumns().size());
    for (ColumnMetadata columnMetadata : tableMetadata.getColumns().values()) {
      // convert the ColumnMetadata to ApiColumnDef, then from there into the ColumnType
      try {
        tableColumns.put(
            columnMetadata.getName(),
            getApiDataType(columnMetadata, tableSchemaObject.vectorConfig()).getColumnType());
      } catch (UnsupportedCqlType e) {
        tableColumns.put(
            columnMetadata.getName(),
            new ComplexColumnType.UnsupportedType(columnMetadata.getType()));
      }
    }

    var partitionSort =
        tableMetadata.getClusteringColumns().entrySet().stream()
            .map(entry -> PrimaryKey.OrderingKey.from(entry.getKey().getName(), entry.getValue()))
            .toList();
    var partitionBy =
        tableMetadata.getPartitionKey().stream().map(ColumnMetadata::getName).toList();
    var primaryKey = PrimaryKey.from(partitionBy, partitionSort);

    return new TableResponse(
        tableMetadata.getName(), new TableDefinition(tableColumns, primaryKey));
  }

  private ApiDataType getApiDataType(ColumnMetadata columnMetadata, VectorConfig vectorConfig)
      throws UnsupportedCqlType {

    if (columnMetadata.getType() instanceof VectorType vt) {
      // Special handling because we need to get the vectorize config from the vector config

      // TODO: WHAT HAPPENS IF THIS IS NOT PRESENT ???
      // TODO: HACK AARON - remove call is asInternal
      var columnVectorDefinition =
          vectorConfig.columnVectorDefinitions().stream()
              .filter(vc -> vc.fieldName().equals(columnMetadata.getName().asInternal()))
              .findFirst()
              .get();

      return ComplexApiDataType.ApiVectorType.from(
          ApiDataTypeDefs.from(vt.getElementType()),
          vt.getDimensions(),
          columnVectorDefinition.vectorizeDefinition());

    } else {
      return ApiDataTypeDefs.from(columnMetadata.getType());
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
  public record TableResponse(@JsonIgnore CqlIdentifier tableName, TableDefinition definition) {

    public String getName() {
      return cqlIdentifierToJsonKey(tableName);
    }
  }

  @JsonPropertyOrder({"columns", "primaryKey"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  record TableDefinition(
      @JsonIgnore Map<CqlIdentifier, ColumnType> tableColumns, PrimaryKey primaryKey) {

    public Map<String, ColumnType> getColumns() {
      return tableColumns.entrySet().stream()
          .collect(
              Collectors.toMap(
                  entry -> cqlIdentifierToJsonKey(entry.getKey()), Map.Entry::getValue));
    }
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
}
