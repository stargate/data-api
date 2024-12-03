package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;
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
 * @param <JavaT> The JSON Type , BigDecimal, String etc
 */
public abstract class NativeTypeTableFilter<JavaT> extends TableFilter implements PrettyPrintable {

  /**
   * The operations that can be performed to filter a column TIDY: we have operations defined in
   * multiple places, once we have refactored the collection operations we should centralize these
   * operator definitions
   *
   * <p>TODO, other operators that apply to scaler / native tyes
   */
  public enum Operator {
    EQ(BuiltConditionPredicate.EQ),
    NE(BuiltConditionPredicate.NEQ),
    LT(BuiltConditionPredicate.LT),
    GT(BuiltConditionPredicate.GT),
    LTE(BuiltConditionPredicate.LTE),
    GTE(BuiltConditionPredicate.GTE);

    public final BuiltConditionPredicate predicate;

    Operator(BuiltConditionPredicate predicate) {
      this.predicate = predicate;
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

    public boolean isComparisonOperator() {
      return this == LTE || this == GTE || this == LT || this == GT;
    }
  }

  public final Operator operator;
  protected final JavaT columnValue;

  protected NativeTypeTableFilter(String path, Operator operator, JavaT columnValue) {
    super(path);
    this.columnValue = columnValue;
    this.operator = operator;
  }

  @Override
  public BuiltCondition get() {
    throw new UnsupportedOperationException(
        "Not supported - will be modified when we migrate collections filters java driver");
  }

  @Override
  public Relation apply(
      TableSchemaObject tableSchemaObject,
      //      StmtT ongoingWhereClause,
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
      throw new RuntimeException(e);
    }

    return Relation.column(getPathAsCqlIdentifier()).build(operator.predicate.cql, bindMarker());
    //    return ongoingWhereClause.where(
    //        Relation.column(getPathAsCqlIdentifier()).build(operator.predicate.cql,
    // bindMarker()));
  }

  @Override
  public String toString() {
    return toString(false);
  }

  public String toString(boolean pretty) {
    return toString(new PrettyToStringBuilder(getClass(), pretty)).toString();
  }

  public PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
    prettyToStringBuilder
        .append("path", path)
        .append("operator", operator)
        .append("columnValue", columnValue);
    return prettyToStringBuilder;
  }

  @Override
  public PrettyToStringBuilder appendTo(PrettyToStringBuilder prettyToStringBuilder) {
    var sb = prettyToStringBuilder.beginSubBuilder(getClass());
    return toString(sb).endSubBuilder();
  }
}
