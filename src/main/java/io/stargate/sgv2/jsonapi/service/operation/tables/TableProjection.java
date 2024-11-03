package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtApiColumnDef;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.querybuilder.select.OngoingSelection;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToJSONCodecException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.OperationProjection;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.service.operation.query.SelectCQLClause;
import io.stargate.sgv2.jsonapi.service.projection.TableProjectionDefinition;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The projection for a read operation.
 *
 * <p>This class does double duty: it implements the {@link SelectCQLClause} interface to apply the
 * projection to the cql statement, and it implements the {@link OperationProjection} interface to
 * apply the projection to rows read from the database.
 *
 * <p>TODO: refactor to use the factory / builder pattern for other clauses and give better errors
 */
public class TableProjection implements SelectCQLClause, OperationProjection {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableProjection.class);

  private ObjectMapper objectMapper;
  private TableSchemaObject table;
  private List<ColumnMetadata> columns;
  private ColumnsDescContainer columnsDesc;

  private TableProjection(
      ObjectMapper objectMapper,
      TableSchemaObject table,
      List<ColumnMetadata> columns,
      ColumnsDescContainer columnsDesc) {
    this.objectMapper = objectMapper;
    this.table = table;
    this.columns = columns;
    this.columnsDesc = columnsDesc;
  }

  /**
   * Factory method for construction projection instance, given a projection definition and table
   * schema.
   */
  public static TableProjection fromDefinition(
      ObjectMapper objectMapper,
      TableProjectionDefinition projectionDefinition,
      TableSchemaObject table) {

    Map<String, ColumnMetadata> columnsByName = new HashMap<>();
    // TODO: This can also be cached as part of TableSchemaObject than resolving it for every query.
    table
        .tableMetadata()
        .getColumns()
        .forEach((id, column) -> columnsByName.put(id.asInternal(), column));

    List<ColumnMetadata> columns = projectionDefinition.extractSelectedColumns(columnsByName);

    // TODO: A table can't be with empty columns. Think a redundant check.
    if (columns.isEmpty()) {
      throw ErrorCodeV1.UNSUPPORTED_PROJECTION_DEFINITION.toApiException(
          "did not include any Table columns");
    }

    // result set has ColumnDefinitions not ColumnMetadata kind of weird

    var readApiColumns =
        table
            .apiTableDef()
            .allColumns()
            .filterBy(columns.stream().map(ColumnMetadata::getName).toList());
    if (!readApiColumns.filterByUnsupported().isEmpty()) {
      throw new IllegalStateException(
          "Unsupported columns in the result set: %s"
              .formatted(errFmtApiColumnDef(readApiColumns.filterByUnsupported())));
    }

    return new TableProjection(objectMapper, table, columns, readApiColumns.toColumnsDesc());
  }

  @Override
  public Select apply(OngoingSelection ongoingSelection) {
    Set<CqlIdentifier> readColumns = new LinkedHashSet<>();
    readColumns.addAll(columns.stream().map(ColumnMetadata::getName).toList());
    return ongoingSelection.columnsIds(readColumns);
  }

  @Override
  public JsonNode projectRow(Row row) {
    long startNano = System.nanoTime();
    int nonNullCount = 0;
    int skippedNullCount = 0;

    ObjectNode result = objectMapper.createObjectNode();
    for (int i = 0, len = columns.size(); i < len; ++i) {
      final ColumnMetadata column = columns.get(i);
      final String columnName = column.getName().asInternal();
      JSONCodec codec;

      // TODO: maybe optimize common case of String, Boolean to avoid conversions, lookups
      try {
        codec = JSONCodecRegistries.DEFAULT_REGISTRY.codecToJSON(table.tableMetadata(), column);
      } catch (MissingJSONCodecException e) {
        throw ErrorCodeV1.UNSUPPORTED_PROJECTION_PARAM.toApiException(
            "Column '%s' has unsupported type '%s'", columnName, column.getType().toString());
      }
      try {
        final Object columnValue = row.getObject(i);
        // We have a choice here: convert into JSON null (explicit) or drop (save space)?
        // For now, do former: may change or make configurable later.
        if (columnValue == null) {
          result.putNull(columnName);
        } else {
          nonNullCount++;
          result.put(columnName, codec.toJSON(objectMapper, columnValue));
        }
      } catch (ToJSONCodecException e) {
        throw ErrorCodeV1.UNSUPPORTED_PROJECTION_PARAM.toApiException(
            e,
            "Column '%s' has invalid value of type '%s': failed to convert to JSON: %s",
            columnName,
            column.getType().toString(),
            e.getMessage());
      }
    }

    if (LOGGER.isDebugEnabled()) {
      double durationMs = (System.nanoTime() - startNano) / 1_000_000.0;
      LOGGER.debug(
          "projectRow() row build durationMs={}, columns.size={}, nonNullCount={}, skippedNullCount={}",
          durationMs,
          columns.size(),
          nonNullCount,
          skippedNullCount);
    }
    return result;
  }

  @Override
  public ColumnsDescContainer getSchemaDescription() {
    return columnsDesc;
  }
}
