package io.stargate.sgv2.jsonapi.service.operation.query;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import io.stargate.sgv2.jsonapi.service.shredding.NamedValue;
import java.util.List;
import java.util.Objects;

/**
 * Assigns a single column a value in a CQL Update statement build with the Java Driver Query
 * Builder.
 *
 * <p>NOTE: This class is designed to set scalar column values, basic strings, ints etc. It should
 * be possible to extend it to support more exotic types like collections and UDT's using the
 * appropriate methods on the {@link OngoingAssignment}.
 *
 * <p>Designed to be used with the {@link UpdateValuesCQLClause} to build the full clause.
 *
 * <p>Supports {@link Deferrable} so that the values needed vectorizing can be deferred until
 * execution time. See {@link #deferredValues()} for docs.
 */
public class ColumnAssignment implements CQLAssignment, Deferrable {

  private final CqlNamedValue namedValue;

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Create a new instance of the class to set the {@code column} to the {@code value} in the
   * specified {@code tableMetadata}.
   *
   * @param tableMetadata The {@link TableMetadata} for the target table.
   * @param column The name of the column to set.
   * @param value the {@link JsonLiteral} value created by shredding the value from the update
   *     clause in the request.
   */
  public ColumnAssignment(CqlNamedValue namedValue) {
    this.namedValue = Objects.requireNonNull(namedValue, "namedValue cannot be null");
  }

  public CqlIdentifier name() {
    return namedValue.name();
  }

  public CqlNamedValue namedValue() {
    return namedValue;
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
    return Assignment.setColumn(namedValue.name(), bindMarker());
  }

  /**
   * Add the value to the list of positional values to bind to the query.
   *
   * <p>Is a separate method to support expansion for collections etc in subtypes.
   *
   * @param positionalValues
   */
  protected void addPositionalValues(List<Object> positionalValues) {

    positionalValues.add(namedValue.value());
    //    var rawValue = value.value();
    //    try {
    //      positionalValues.add(
    //          JSONCodecRegistries.DEFAULT_REGISTRY
    //              .codecToCQL(tableMetadata, column, rawValue)
    //              .toCQL(rawValue));
    //    } catch (MissingJSONCodecException e) {
    //      throw DocumentException.Code.UNSUPPORTED_COLUMN_TYPES.get(
    //          errVars(
    //              TableSchemaObject.from(tableMetadata, OBJECT_MAPPER),
    //              map -> {
    //                map.put("allColumns",
    // errFmtColumnMetadata(tableMetadata.getColumns().values()));
    //                map.put("unsupportedColumns", column.asInternal());
    //              }));
    //    } catch (UnknownColumnException e) {
    //      throw FilterException.Code.UNKNOWN_TABLE_COLUMNS.get(
    //          errVars(
    //              TableSchemaObject.from(tableMetadata, OBJECT_MAPPER),
    //              map -> {
    //                map.put("allColumns",
    // errFmtColumnMetadata(tableMetadata.getColumns().values()));
    //                map.put("unknownColumns", CqlIdentifierUtil.cqlIdentifierToJsonKey(column));
    //              }));
    //    } catch (ToCQLCodecException e) {
    //      throw UpdateException.Code.INVALID_UPDATE_COLUMN_VALUES.get(
    //          errVars(
    //              TableSchemaObject.from(tableMetadata, OBJECT_MAPPER),
    //              map -> {
    //                map.put("allColumns",
    // errFmtColumnMetadata(tableMetadata.getColumns().values()));
    //                map.put("invalidColumn", CqlIdentifierUtil.cqlIdentifierToJsonKey(column));
    //                map.put("columnType",
    // tableMetadata.getColumn(column).get().getType().toString());
    //              }));
    //    }
  }

  @Override
  public List<? extends NamedValue<?, ?, ?>> deferredValues() {
    return new CqlNamedValueContainer(List.of(namedValue)).deferredValues();
  }
}
