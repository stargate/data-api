package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.service.operation.query.FilterBehaviour;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.List;

/**
 * A DB Filter that can be applied on columns in a CQL Tables that use the `native-type` 's as
 * defined in the CQL specification.
 *
 * <pre>
 *   <native-type> ::= ascii
 *                 | bigint
 *                 | blob
 *                 | boolean
 *                 | counter
 *                 | date
 *                 | decimal
 *                 | double
 *                 | duration
 *                 | float
 *                 | inet
 *                 | int
 *                 | smallint
 *                 | text
 *                 | time
 *                 | timestamp
 *                 | timeuuid
 *                 | tinyint
 *                 | uuid
 *                 | varchar
 *                 | varint
 * </pre>
 *
 * @param <CqlT> The JSON Type , BigDecimal, String etc
 */
public abstract class NativeTypeTableFilter<CqlT> extends TableFilter implements Recordable {

  /**
   * The operations that can be performed to filter a column TIDY: we have operations defined in
   * multiple places, once we have refactored the collection operations we should centralize these
   * operator definitions
   *
   * <p>TODO, other operators that apply to scalar / native types
   */
  public enum Operator {
    EQ(BuiltConditionPredicate.EQ, new FilterBehaviour.Behaviour(true, false)),
    NE(BuiltConditionPredicate.NEQ, new FilterBehaviour.Behaviour(false, false)),
    LT(BuiltConditionPredicate.LT, new FilterBehaviour.Behaviour(false, true)),
    GT(BuiltConditionPredicate.GT, new FilterBehaviour.Behaviour(false, true)),
    LTE(BuiltConditionPredicate.LTE, new FilterBehaviour.Behaviour(false, true)),
    GTE(BuiltConditionPredicate.GTE, new FilterBehaviour.Behaviour(false, true));

    public final BuiltConditionPredicate predicate;
    public final FilterBehaviour filterBehaviour;

    Operator(BuiltConditionPredicate predicate, FilterBehaviour filterBehaviour) {
      this.predicate = predicate;
      this.filterBehaviour = filterBehaviour;
    }

    public static Operator from(ValueComparisonOperator operator) {
      return switch (operator) {
        case EQ -> EQ;
        case NE -> NE;
        case GT -> GT;
        case GTE -> GTE;
        case LT -> LT;
        case LTE -> LTE;
        default -> throw new IllegalArgumentException("Unsupported operator: " + operator);
      };
    }
  }

  public final Operator operator;
  protected final CqlT columnValue;

  protected NativeTypeTableFilter(String path, Operator operator, CqlT columnValue) {
    super(path, operator.filterBehaviour);
    this.columnValue = columnValue;
    this.operator = operator;
  }

  @Override
  public BuiltCondition get() {
    throw new UnsupportedOperationException(
        "Not supported - will be modified when we migrate collections filters java driver");
  }

  @Override
  public <StmtT extends OngoingWhereClause<StmtT>> StmtT apply(
      TableSchemaObject tableSchemaObject,
      StmtT ongoingWhereClause,
      List<Object> positionalValues) {

    try {
      var codec =
          JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
              tableSchemaObject.tableMetadata(), getPathAsCqlIdentifier(), columnValue);
      positionalValues.add(codec.toCQL(columnValue));
    } catch (UnknownColumnException e) {
      throw FilterException.Code.UNKNOWN_TABLE_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "allColumns",
                    errFmtColumnMetadata(tableSchemaObject.tableMetadata().getColumns().values()));
                map.put("unknownColumns", path);
              }));
    } catch (MissingJSONCodecException e) {
      throw DocumentException.Code.UNSUPPORTED_COLUMN_TYPES.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "allColumns",
                    errFmtColumnMetadata(tableSchemaObject.tableMetadata().getColumns().values()));
                map.put("unsupportedColumns", path);
              }));
    } catch (ToCQLCodecException e) {
      throw FilterException.Code.INVALID_FILTER_COLUMN_VALUES.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "allColumns",
                    errFmtColumnMetadata(tableSchemaObject.tableMetadata().getColumns().values()));
                map.put("invalidColumn", path);
                map.put(
                    "columnType",
                    tableSchemaObject.tableMetadata().getColumn(path).get().getType().toString());
              }));
    }

    return ongoingWhereClause.where(
        Relation.column(getPathAsCqlIdentifier()).build(operator.predicate.cql, bindMarker()));
  }

  public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    return dataRecorder
        .append("path", path)
        .append("operator", operator)
        .append("columnValue", columnValue);
  }
}
