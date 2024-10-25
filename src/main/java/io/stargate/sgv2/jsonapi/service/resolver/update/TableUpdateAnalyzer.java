package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnMetadata;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Analyzer for table update command.<br>
 * For Table UpdateOne update clause, there are several rules to check as followings:
 *
 * <p>1. rule checkUnknownColumns(). Can not update on unknown columns. <br>
 * 2. rule checkUpdateOnPrimaryKey(). Can not update on PK columns. <br>
 */
public class TableUpdateAnalyzer {

  private final TableBasedSchemaObject tableSchemaObject;
  private final TableMetadata tableMetadata;
  private final Map<CqlIdentifier, ColumnMetadata> tablePKColumns;
  private final Map<CqlIdentifier, ColumnMetadata> tableAllColumns;

  public TableUpdateAnalyzer(TableBasedSchemaObject tableSchemaObject) {
    this.tableSchemaObject =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    tableMetadata = tableSchemaObject.tableMetadata();
    tablePKColumns =
        tableMetadata.getPrimaryKey().stream()
            .collect(Collectors.toMap(ColumnMetadata::getName, Function.identity()));
    tableAllColumns = tableMetadata.getColumns();
  }

  public void analyze(List<ColumnAssignment> columnAssignments) {
    // check rules
    checkUnknownColumns(columnAssignments);
    checkUpdateOnPrimaryKey(columnAssignments);
  }

  /** Update on unknown column is not allowed * */
  private void checkUnknownColumns(List<ColumnAssignment> columnAssignments) {
    List<CqlIdentifier> unknownColumns =
        columnAssignments.stream()
            .filter(columnAssignment -> !tableAllColumns.containsKey(columnAssignment.column))
            .map(columnAssignment -> columnAssignment.column)
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (!unknownColumns.isEmpty()) {
      throw UpdateException.Code.UNKNOWN_TABLE_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("unknownColumns", errFmtCqlIdentifier(unknownColumns));
                // use tableAllColumns from tableMetadata, preserve the order in tableMetadata
                map.put("allColumns", errFmtColumnMetadata(tableAllColumns.values()));
              }));
    }
  }

  /** Update primary key columns is not allowed. */
  private void checkUpdateOnPrimaryKey(List<ColumnAssignment> columnAssignments) {
    List<CqlIdentifier> invalidUpdatePKColumns =
        columnAssignments.stream()
            .filter(columnAssignment -> tablePKColumns.containsKey(columnAssignment.column))
            .map(columnAssignment -> columnAssignment.column)
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (!invalidUpdatePKColumns.isEmpty()) {
      throw UpdateException.Code.UPDATE_PRIMARY_KEY_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("updateOnPrimaryKeyColumns", errFmtCqlIdentifier(invalidUpdatePKColumns));
                map.put("primaryKeys", errFmtColumnMetadata(tableMetadata.getPrimaryKey()));
              }));
    }
  }
}
