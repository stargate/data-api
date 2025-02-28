package io.stargate.sgv2.jsonapi.service.operation.query;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnMetadata;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

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

  //  private static final Map<
  //          UpdateOperator, BiFunction<OngoingAssignment, CqlIdentifier, UpdateWithAssignments>>
  //          SUPPORTED_UPDATE_OPERATOR_MAP =
  //          Map.of(
  //              UpdateOperator.SET, ColumnAssignment::resolveSetUnsetToAssignment,
  //              UpdateOperator.UNSET, ColumnAssignment::resolveSetUnsetToAssignment,
  //              UpdateOperator.PUSH, ColumnAssignment::resolvePushToAssignment,
  //              UpdateOperator.PULL_ALL, ColumnAssignment::resolvePullAllToAssignment);

  private final BiFunction<OngoingAssignment, CqlIdentifier, UpdateWithAssignments>
      updateToAssignment;
  private final TableMetadata tableMetadata;
  public final CqlIdentifier column;
  private final JsonLiteral<?> value;

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Create a new instance of the class to set the {@code column} to the {@code value} in the
   * specified {@code tableMetadata}.
   *
   * @param updateToAssignment The BiFunction to add update to the ongoing assignment.
   * @param tableMetadata The {@link TableMetadata} for the target table.
   * @param column The name of the column to set.
   * @param value the {@link JsonLiteral} value created by shredding the value from the update
   *     clause in the request.
   */
  public ColumnAssignment(
      BiFunction<OngoingAssignment, CqlIdentifier, UpdateWithAssignments> updateToAssignment,
      TableMetadata tableMetadata,
      CqlIdentifier column,
      JsonLiteral<?> value) {
    this.updateToAssignment = updateToAssignment;
    this.tableMetadata = Objects.requireNonNull(tableMetadata, "tableMetadata cannot be null");
    this.column = Objects.requireNonNull(column, "column cannot be null");
    // Value may be null, this is how to clear a column in CQL
    this.value = value;
  }

  @Override
  public UpdateWithAssignments apply(
      OngoingAssignment ongoingAssignment, List<Object> positionalValues) {

    addPositionalValues(positionalValues);
    return updateToAssignment.apply(ongoingAssignment, column);
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

      // special case for $pullAll against a map column.
      // We only need to get the keyCodec
      if (updateToAssignment instanceof ColumnRemoveToAssignment
          && tableMetadata.getColumn(column).get().getType() instanceof MapType) {
        List<JsonLiteral<?>> jsonValues = (List<JsonLiteral<?>>) rawValue;
        List<Object> cqlValues = new ArrayList<>();
        for (JsonLiteral<?> jsonValue : jsonValues) {
          JSONCodec<Object, Object> keyCodec =
              JSONCodecRegistries.DEFAULT_REGISTRY.getKeyCodecForMap(
                  tableMetadata, column, jsonValue.value());
          cqlValues.add(keyCodec.toCQL(jsonValue.value()));
        }
        positionalValues.add(cqlValues);
        return;
      }

      positionalValues.add(
          JSONCodecRegistries.DEFAULT_REGISTRY
              .codecToCQL(tableMetadata, column, rawValue)
              .toCQL(rawValue));
    } catch (MissingJSONCodecException e) {
      throw DocumentException.Code.UNSUPPORTED_COLUMN_TYPES.get(
          errVars(
              TableSchemaObject.from(tableMetadata, OBJECT_MAPPER),
              map -> {
                map.put("allColumns", errFmtColumnMetadata(tableMetadata.getColumns().values()));
                map.put("unsupportedColumns", column.asInternal());
              }));
    } catch (UnknownColumnException e) {
      throw UpdateException.Code.UNKNOWN_TABLE_COLUMNS.get(
          errVars(
              TableSchemaObject.from(tableMetadata, OBJECT_MAPPER),
              map -> {
                map.put("allColumns", errFmtColumnMetadata(tableMetadata.getColumns().values()));
                map.put("unknownColumns", CqlIdentifierUtil.cqlIdentifierToJsonKey(column));
              }));
    } catch (ToCQLCodecException e) {
      throw UpdateException.Code.INVALID_UPDATE_COLUMN_VALUES.get(
          errVars(
              TableSchemaObject.from(tableMetadata, OBJECT_MAPPER),
              map -> {
                map.put("allColumns", errFmtColumnMetadata(tableMetadata.getColumns().values()));
                map.put("invalidColumn", CqlIdentifierUtil.cqlIdentifierToJsonKey(column));
                map.put("columnType", tableMetadata.getColumn(column).get().getType().toString());
                map.put("embeddedCodecMessage", e.getMessage());
              }));
    }
  }

  /** This method is used for unit test in TableUpdateOperatorTest */
  public boolean testEquals(UpdateOperator updateOperator, JsonLiteral<?> value) {
    if (updateToAssignment instanceof ColumnAppendToAssignment
        && updateOperator == UpdateOperator.PUSH) {
      return this.value.equals(value);
    } else if (updateToAssignment instanceof ColumnRemoveToAssignment
        && updateOperator == UpdateOperator.PULL_ALL) {
      return this.value.equals(value);
    } else if (updateToAssignment instanceof ColumnSetToAssignment
        && (updateOperator == UpdateOperator.SET || updateOperator == UpdateOperator.UNSET)) {
      return this.value.equals(value);
    }
    return false;
  }
}
