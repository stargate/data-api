package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.FromJavaCodecException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
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
 * @param <T> The JSON Type , BigDecimal, String etc
 */
public abstract class NativeTypeTableFilter<T> extends TableFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NativeTypeTableFilter.class);

  /**
   * The operations that can be performed to filter a column TIDY: we have operations defined in
   * multiple places, once we have refactored the collection operations we should centralize these
   * operator definitions
   */
  public enum Operator {
    // TODO, other operators like  NE, IN etc.
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
  protected final T columnValue;

  protected NativeTypeTableFilter(String path, Operator operator, T columnValue) {
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
  public Select apply(
      TableSchemaObject tableSchemaObject, Select select, List<Object> positionalValues) {

    // TODO: AARON return the correct errors, this is POC work now
    // TODO: Checking for valid column should be part of request deserializer or to be done in
    // resolver. Should not be left till operation classes.
    var column =
        tableSchemaObject
            .tableMetadata
            .getColumn(path)
            .orElseThrow(() -> new IllegalArgumentException("Column not found: " + path));

    var codec =
        JSONCodecRegistry.codecFor(column.getType(), columnValue)
            .orElseThrow(
                () ->
                    ErrorCode.ERROR_APPLYING_CODEC.toApiException(
                        "No Codec for a value of type %s with table column %s it has CQL type %s",
                        columnValue.getClass(),
                        column.getName(),
                        column.getType().asCql(true, false)));

    try {
      positionalValues.add(codec.apply(columnValue));
    } catch (FromJavaCodecException e) {
      throw ErrorCode.ERROR_APPLYING_CODEC.toApiException(e, "Error applying codec");
    }

    return select.where(Relation.column(path).build(operator.predicate.cql, bindMarker()));
  }
}
