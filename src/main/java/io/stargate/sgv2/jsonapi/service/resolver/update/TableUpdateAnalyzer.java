package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnMetadata;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;
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
 * <p>rule checkUpdateOnPrimaryKey(). Can not update on PK columns. aaron -19 march 2025 - a number
 * of rules were moved from there into the {@link
 * io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue.ErrorStrategy}
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
    // NOTE:assume all column assignments came from bound CQLNamedValues, so we do not check for
    // unknown columns.
    checkUpdateOnPrimaryKey(columnAssignments);
  }

  /** Update primary key columns is not allowed. */
  private void checkUpdateOnPrimaryKey(List<ColumnAssignment> columnAssignments) {
    List<CqlIdentifier> invalidUpdatePKColumns =
        columnAssignments.stream()
            .map(ColumnAssignment::name)
            .filter(name -> tablePKColumns.containsKey(name))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (!invalidUpdatePKColumns.isEmpty()) {
      throw UpdateException.Code.UNSUPPORTED_UPDATE_FOR_PRIMARY_KEY_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("updateOnPrimaryKeyColumns", errFmtCqlIdentifier(invalidUpdatePKColumns));
                map.put("primaryKeys", errFmtColumnMetadata(tableMetadata.getPrimaryKey()));
              }));
    }
  }
}
