package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.catchable.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.service.operation.query.ExtendedOngoingWhereClause;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
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
public abstract class NativeTypeTableFilter<CqlT> extends TableFilter {
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

    final BuiltConditionPredicate predicate;

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
  public <StmtT extends OngoingWhereClause<StmtT>> ExtendedOngoingWhereClause<StmtT> apply(
      TableSchemaObject tableSchemaObject,
      ExtendedOngoingWhereClause<StmtT> extendedOngoingWhereClause,
      List<Object> positionalValues) {

    try {
      var codec =
          JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
              tableSchemaObject.tableMetadata(), getPathAsCqlIdentifier(), columnValue);
      positionalValues.add(codec.toCQL(columnValue));
    } catch (UnknownColumnException e) {
      throw ErrorCodeV1.TABLE_COLUMN_UNKNOWN.toApiException(e.getMessage());
    } catch (MissingJSONCodecException e) {
      throw ErrorCodeV1.TABLE_COLUMN_TYPE_UNSUPPORTED.toApiException(e.getMessage());
    } catch (ToCQLCodecException e) {
      // TODO AARON - Handle error
      throw new RuntimeException(e);
    }

    return extendedOngoingWhereClause.where(
        Relation.column(getPathAsCqlIdentifier()).build(operator.predicate.cql, bindMarker()),
        shouldAddAllowFiltering(tableSchemaObject));
  }

  /**
   * shouldAddAllowFiltering implementation for all nativeTypeTableFilter
   *
   * <p>[NativeTypeTableFilter on primary key column].<br>
   * Example Table definition: {"definition": {"columns": {"id": {"type": "int"},"age": {"type":
   * "int"},"name": {"type": "text"}},"primaryKey": "id"}}.<br>
   * API filter($eq) on id(primary key column) does NOT need ALLOW FILTERING without SAI index,
   * already a primary key index behind the scene. Note, other API filters($ne, $lt, $gt, etc..)
   * still need ALLOW FILTERING if without SAI index.
   *
   * <p>[NativeTypeTableFilter on non-primary key, scalar column, without SAI index/primary key].
   * <br>
   * Example Table definition: {"definition": {"columns": {"id": {"type": "int"},"age": {"type":
   * "int"},"name": {"type": "text"}},"primaryKey": "id"}}.<br>
   * If without SAI index, All nativeTypeTableFilter on these non-primary key scalar column need
   * ALLOW FILTERING.
   *
   * <p>[NativeTypeTableFilter on non-primary key, scalar column, with SAI index/primary key].<br>
   * Example Table definition: {"definition": {"columns": {"id": {"type": "int"},"age": {"type":
   * "int"},"name": {"type": "text"}},"primaryKey": "id"}}.<br>
   * With SAI index, there are several situations we still need ALLOW FILTERING.<br>
   * (1) $ne on scalar column ...
   *
   * @param tableSchemaObject tableSchemaObject
   * @return boolean to indicate ALLOW FILTERING is needed or not
   */
  @Override
  public boolean shouldAddAllowFiltering(TableSchemaObject tableSchemaObject) {

    // if column is on the primary key, does not need ALLOW FILTERING to perform $eq
    if (hasPrimaryKeyOnColumn(tableSchemaObject) && operator == Operator.EQ) {
      return false;
    }

    // if column is on the index, there are several situations that we still need allow filtering
    if (hasSaiIndexOnColumn(tableSchemaObject)) {

      if (operator == Operator.NE) {
        return true;
      }
      // TODO other operators?

      return false;
    }

    // If without index, then all filters on scalar column need ALLOW FILTERING
    return true;
  }
}
