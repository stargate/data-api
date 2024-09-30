package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
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
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilterAnalyzedUsage;
import java.util.List;
import java.util.Optional;

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
      throw ErrorCodeV1.TABLE_COLUMN_UNKNOWN.toApiException(e.getMessage());
    } catch (MissingJSONCodecException e) {
      throw ErrorCodeV1.TABLE_COLUMN_TYPE_UNSUPPORTED.toApiException(e.getMessage());
    } catch (ToCQLCodecException e) {
      // TODO AARON - Handle error
      throw new RuntimeException(e);
    }

    return ongoingWhereClause.where(
        Relation.column(getPathAsCqlIdentifier()).build(operator.predicate.cql, bindMarker()));
  }

  /**
   * Analyze the $eq,$ne,$lt,$gt,$lte,$gte against scalar column and get corresponding usage info.
   *
   * <p>Check if the filter is against an existing column by calling getColumn() on {@link
   * TableFilter}
   *
   * <p>For comparison API filters, $lt/$gt/$lte/$gte. <br>
   * Can NOT use on Duration column, "Slice restrictions are not supported on duration columns" <br>
   * If no SAI index on following 14 column types
   * text/int/timestamp/ascii/date/time/boolean/varint/tinyint/decimal/smallint/double/bigint/float,
   * need ALLOW FILTERING. <br>
   * TODO, blob
   *
   * <p>For $ne <br>
   * If has SAI on following 10 columns,
   * date/time/timestamp/tinyint/smallint/bigint/varint/float/double/decinal, ALLOW FILTERING is not
   * needed. <br>
   * For other column types, WITH or WITHOUT SAI, ALLOWING FILTERING is needed. TODO, blob
   *
   * <p>For $eq <br>
   * With SAI index on these 14 columns,
   * date/time/timestamp/int/tinyint/smallint/bigint/varint/float/double/decimal/text/ascii/boolean,
   * ALLOW FILTERING is not needed. We can NOT build SAI index on duration column type, so ALLOW
   * FILTERING is also needed. TODO, blob
   */
  @Override
  public TableFilterAnalyzedUsage analyze(TableSchemaObject tableSchemaObject) {
    // check if filter is against an existing column
    final ColumnMetadata column = getColumn(tableSchemaObject);

    // if column is on the primary key, does not need ALLOW FILTERING to perform $eq
    if (hasPrimaryKeyOnColumn(tableSchemaObject) && operator == Operator.EQ) {
      return new TableFilterAnalyzedUsage(path, false, Optional.empty());
    }

    if (operator == Operator.LT
        || operator == Operator.GT
        || operator == Operator.LTE
        || operator == Operator.GTE) {
      // Slice restrictions are not supported on duration columns (with or without index)
      if (column.getType().equals(DataTypes.DURATION)) {
        // TODO, do we need to explain detail here, like "Slice restrictions are not supported on
        // duration columns"
        throw ErrorCodeV1.TABLE_INVALID_FILTER.toApiException(
            "'%s' is not supported on duration column '%s', Slice restrictions are not supported on duration columns.",
            operator.predicate.cql, path);
      }

      if (!hasSaiIndexOnColumn(tableSchemaObject)) {
        return new TableFilterAnalyzedUsage(path, true, Optional.of("ALLOW FILTERING turned on"));
      }
      return new TableFilterAnalyzedUsage(path, false, Optional.empty());
    }

    // Check special cases for API filter $eq
    if (operator == Operator.NE) {
      if (hasSaiIndexOnColumn(tableSchemaObject)) {
        if (column.getType().equals(DataTypes.DATE)
            || column.getType().equals(DataTypes.TIME)
            || column.getType().equals(DataTypes.TIMESTAMP)
            || column.getType().equals(DataTypes.INT)
            || column.getType().equals(DataTypes.TINYINT)
            || column.getType().equals(DataTypes.SMALLINT)
            || column.getType().equals(DataTypes.BIGINT)
            || column.getType().equals(DataTypes.VARINT)
            || column.getType().equals(DataTypes.FLOAT)
            || column.getType().equals(DataTypes.DOUBLE)
            || column.getType().equals(DataTypes.DECIMAL)) {
          return new TableFilterAnalyzedUsage(path, false, Optional.empty());
        }
      }
      return new TableFilterAnalyzedUsage(path, true, Optional.of("ALLOW FILTERING turned on"));
    }

    if (operator == Operator.EQ) {
      if (hasSaiIndexOnColumn(tableSchemaObject)) {
        if (column.getType().equals(DataTypes.DATE)
            || column.getType().equals(DataTypes.TIME)
            || column.getType().equals(DataTypes.TIMESTAMP)
            || column.getType().equals(DataTypes.INT)
            || column.getType().equals(DataTypes.TINYINT)
            || column.getType().equals(DataTypes.SMALLINT)
            || column.getType().equals(DataTypes.BIGINT)
            || column.getType().equals(DataTypes.VARINT)
            || column.getType().equals(DataTypes.FLOAT)
            || column.getType().equals(DataTypes.DOUBLE)
            || column.getType().equals(DataTypes.DECIMAL)
            || column.getType().equals(DataTypes.TEXT)
            || column.getType().equals(DataTypes.ASCII)
            || column.getType().equals(DataTypes.BOOLEAN)) {
          return new TableFilterAnalyzedUsage(path, false, Optional.empty());
        }
      }
      return new TableFilterAnalyzedUsage(path, true, Optional.of("ALLOW FILTERING turned on"));
    }

    return new TableFilterAnalyzedUsage(path, false, Optional.empty());
  }
}
