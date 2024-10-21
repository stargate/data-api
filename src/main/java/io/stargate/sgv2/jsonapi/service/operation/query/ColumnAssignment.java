package io.stargate.sgv2.jsonapi.service.operation.query;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.exception.catchable.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Assigns a single column a value in a CQL Update statement build with the Java Driver Query
 * Builder.
 *
 * <p>NOTE: This class is designed to set scalar column values, basic strings, ints etc. It should
 * be possible to extend it to support more exotic types like collections and UDT's using the
 * appropriate methods on the {@link OngoingAssignment}.
 *
 * <p>Designed to be used with the {@link UpdateValuesCQLClause} to build the full clause.
 */
public class ColumnAssignment implements CQLAssignment {

  private final TableBasedSchemaObject schemaObject;
  private final TableMetadata tableMetadata;
  private final CqlIdentifier column;
  private final JsonLiteral<?> value;

  /**
   * Create a new instance of the class to set the {@code column} to the {@code value} in the
   * specified {@code tableMetadata}.
   *
   * @param tableSchemaObject The {@link TableBasedSchemaObject} for the target table.
   * @param column The name of the column to set.
   * @param value the {@link JsonLiteral} value created by shredding the value from the update
   *     clause in the request.
   */
  public ColumnAssignment(
      TableBasedSchemaObject tableSchemaObject, CqlIdentifier column, JsonLiteral<?> value) {
    this.schemaObject =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    this.column = Objects.requireNonNull(column, "column cannot be null");
    // Value may be null, this is how to clear a column in CQL
    this.value = value;
    this.tableMetadata = schemaObject.tableMetadata();
    assignmentRulesCheck();
  }

  @Override
  public UpdateWithAssignments apply(
      OngoingAssignment ongoingAssignment, List<Object> positionalValues) {

    addPositionalValues(positionalValues);
    return ongoingAssignment.set(getAssignment());
  }

  /**
   * Get the {@link Assignment} for the column and value.
   *
   * <p>Is a separate method to support expansion for collections etc in subtypes.
   *
   * @return
   */
  protected Assignment getAssignment() {
    return Assignment.setColumn(column, bindMarker());
  }

  /**
   * Add the value to the list of positional values to bind to the query.
   *
   * <p>Is a separate method to support expansion for collections etc in subtypes.
   *
   * @param positionalValues
   */
  protected void addPositionalValues(List<Object> positionalValues) {

    var rawValue = value.value();
    try {
      positionalValues.add(
          JSONCodecRegistries.DEFAULT_REGISTRY
              .codecToCQL(tableMetadata, column, rawValue)
              .toCQL(rawValue));
    } catch (MissingJSONCodecException e) {
      // TODO: Better error handling
      throw new RuntimeException(e);
    } catch (UnknownColumnException e) {
      // TODO: Better error handling
      throw new RuntimeException(e);
    } catch (ToCQLCodecException e) {
      throw new RuntimeException(e);
    }
  }

  // For table UpdateOne, there are several rules to check for valid update clause
  // TODO, is this the right place, or we want a similar analyzer like filterAnalyzer
  private void assignmentRulesCheck() {
    var tablePKColumns =
        tableMetadata.getPrimaryKey().stream()
            .collect(Collectors.toMap(ColumnMetadata::getName, Function.identity()));
    var allColumns = tableMetadata.getColumns();

    // check rules
    checkUpdateOnNonExistedColumn(tablePKColumns, allColumns);
    checkUpdateOnPrimaryKey(tablePKColumns, allColumns);
  }

  /** Update on table non-exited column is not allowed * */
  private void checkUpdateOnNonExistedColumn(
      Map<CqlIdentifier, ColumnMetadata> tablePKColumns,
      Map<CqlIdentifier, ColumnMetadata> allColumns) {
    if (!allColumns.containsKey(column)) {
      throw UpdateException.Code.UPDATE_UNKNOWN_TABLE_COLUMN.get(
          errVars(
              schemaObject,
              map -> {
                map.put("unknownColumn", errFmtCqlIdentifier(List.of(column)));
                map.put("allColumns", errFmtColumnMetadata(tableMetadata.getColumns().values()));
              }));
    }
  }

  /** Update a primary key component is not allowed. */
  private void checkUpdateOnPrimaryKey(
      Map<CqlIdentifier, ColumnMetadata> tablePKColumns,
      Map<CqlIdentifier, ColumnMetadata> allColumns) {
    if (tablePKColumns.containsKey(column)) {
      throw UpdateException.Code.UPDATE_PRIMARY_KEY_COLUMN.get(
          errVars(
              schemaObject,
              map -> {
                map.put("updateOnPrimaryKey", errFmtCqlIdentifier(List.of(column)));
                map.put("primaryKeys", errFmtColumnMetadata(tableMetadata.getPrimaryKey()));
              }));
    }
  }
}
