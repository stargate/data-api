package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class NativeTypeTableFilter<CqlT> extends TableFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NativeTypeTableFilter.class);

  /**
   * The operations that can be performed to filter a column TIDY: we have operations defined in
   * multiple places, once we have refactored the collection operations we should centralize these
   * operator definitions
   *
   * <p>TODO, other operators that apply to scaler / native tyes
   */
  public enum Operator {
    EQ(BuiltConditionPredicate.EQ),
    LT(BuiltConditionPredicate.LT),
    GT(BuiltConditionPredicate.GT),
    LTE(BuiltConditionPredicate.LTE),
    GTE(BuiltConditionPredicate.GTE);

    final BuiltConditionPredicate predicate;

    Operator(BuiltConditionPredicate predicate) {
      this.predicate = predicate;
    }

    public static Operator from(ValueComparisonOperator operator) {
      return switch (operator) {
        case EQ -> EQ;
        case GT -> GT;
        case GTE -> GTE;
        case LT -> LT;
        case LTE -> LTE;
        default -> throw new IllegalArgumentException("Unsupported operator: " + operator);
      };
    }
  }

  protected final Operator operator;
  protected final CqlT columnValue;

  protected NativeTypeTableFilter(String path, Operator operator, CqlT columnValue) {
    super(path);
    this.columnValue = columnValue;
    this.operator = operator;
  }

  @Override
  public BuiltCondition get() {
    throw new UnsupportedOperationException(
        "No supported - will be modified when we migrate collections filters java driver");
  }

  @Override
  public <StmtT extends OngoingWhereClause<StmtT>> StmtT apply(
      TableSchemaObject tableSchemaObject,
      StmtT ongoingWhereClause,
      List<Object> positionalValues) {

    try {
      var codec =
          JSONCodecRegistry.codecToCQL(
              tableSchemaObject.tableMetadata, CqlIdentifier.fromCql(path), columnValue);
      positionalValues.add(codec.toCQL(columnValue));
    } catch (UnknownColumnException e) {
      throw ErrorCode.TABLE_COLUMN_UNKNOWN.toApiException(e.getMessage());
    } catch (MissingJSONCodecException e) {
      // TODO AARON - Handle error
      throw new RuntimeException(e);
    } catch (ToCQLCodecException e) {
      // TODO AARON - Handle error
      throw new RuntimeException(e);
    }

    return ongoingWhereClause.where(
        Relation.column(path).build(operator.predicate.cql, bindMarker()));
  }
}
