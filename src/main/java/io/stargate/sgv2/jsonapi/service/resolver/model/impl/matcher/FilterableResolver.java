package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.*;

/**
 * Base for resolvers that are {@link Filterable}, there are a number of commands like find,
 * findOne, updateOne that all have a filter.
 *
 * <p>There will be some re-use, and some customisation to work out.
 *
 * <p>T - type of the command we are resolving
 */
public abstract class FilterableResolver<T extends Command & Filterable> {

  private final FilterMatchRules<T> matchRules = new FilterMatchRules<>();

  private static final Object ID_GROUP = new Object();
  private static final Object ID_GROUP_IN = new Object();
  private static final Object ID_GROUP_RANGE = new Object();
  private static final Object DYNAMIC_GROUP_IN = new Object();
  private static final Object DYNAMIC_TEXT_GROUP = new Object();
  private static final Object DYNAMIC_NUMBER_GROUP = new Object();
  private static final Object DYNAMIC_BOOL_GROUP = new Object();
  private static final Object DYNAMIC_NULL_GROUP = new Object();
  private static final Object DYNAMIC_DATE_GROUP = new Object();
  private static final Object EXISTS_GROUP = new Object();
  private static final Object ALL_GROUP = new Object();
  private static final Object NOT_ANY_GROUP = new Object();
  private static final Object SIZE_GROUP = new Object();
  private static final Object ARRAY_EQUALS = new Object();
  private static final Object SUB_DOC_EQUALS = new Object();

  @Inject OperationsConfig operationsConfig;

  @Inject
  public FilterableResolver() {
    matchRules.addMatchRule(FilterableResolver::findNoFilter, FilterMatcher.MatchStrategy.EMPTY);
    matchRules
        .addMatchRule(FilterableResolver::findById, FilterMatcher.MatchStrategy.STRICT)
        .matcher()
        .capture(ID_GROUP)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.EQ), JsonType.DOCUMENT_ID);

    matchRules
        .addMatchRule(FilterableResolver::findById, FilterMatcher.MatchStrategy.STRICT)
        .matcher()
        .capture(ID_GROUP_IN)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY);

    matchRules
        .addMatchRule(FilterableResolver::findDynamic, FilterMatcher.MatchStrategy.GREEDY)
        .matcher()
        .capture(ID_GROUP)
        .compareValues(
            "_id",
            EnumSet.of(ValueComparisonOperator.EQ, ValueComparisonOperator.NE),
            JsonType.DOCUMENT_ID)
        .capture(ID_GROUP_IN)
        .compareValues(
            "_id",
            EnumSet.of(ValueComparisonOperator.IN, ValueComparisonOperator.NIN),
            JsonType.ARRAY)
        .capture(ID_GROUP_RANGE)
        .compareValues(
            "_id",
            EnumSet.of(
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.DOCUMENT_ID)
        .capture(DYNAMIC_GROUP_IN)
        .compareValues(
            "*",
            EnumSet.of(ValueComparisonOperator.IN, ValueComparisonOperator.NIN),
            JsonType.ARRAY)
        .capture(DYNAMIC_NUMBER_GROUP)
        .compareValues(
            "*",
            EnumSet.of(
                ValueComparisonOperator.EQ,
                ValueComparisonOperator.NE,
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.NUMBER)
        .capture(DYNAMIC_TEXT_GROUP)
        .compareValues(
            "*",
            EnumSet.of(ValueComparisonOperator.EQ, ValueComparisonOperator.NE),
            JsonType.STRING)
        .capture(DYNAMIC_BOOL_GROUP)
        .compareValues(
            "*",
            EnumSet.of(ValueComparisonOperator.EQ, ValueComparisonOperator.NE),
            JsonType.BOOLEAN)
        .capture(DYNAMIC_NULL_GROUP)
        .compareValues(
            "*", EnumSet.of(ValueComparisonOperator.EQ, ValueComparisonOperator.NE), JsonType.NULL)
        .capture(DYNAMIC_DATE_GROUP)
        .compareValues(
            "*",
            EnumSet.of(
                ValueComparisonOperator.EQ,
                ValueComparisonOperator.NE,
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.DATE)
        .capture(EXISTS_GROUP)
        .compareValues("*", EnumSet.of(ElementComparisonOperator.EXISTS), JsonType.BOOLEAN)
        .capture(ALL_GROUP)
        .compareValues("*", EnumSet.of(ArrayComparisonOperator.ALL), JsonType.ARRAY)
        .capture(NOT_ANY_GROUP)
        .compareValues("*", EnumSet.of(ArrayComparisonOperator.NOTANY), JsonType.ARRAY)
        .capture(SIZE_GROUP)
        .compareValues("*", EnumSet.of(ArrayComparisonOperator.SIZE), JsonType.NUMBER)
        .capture(ARRAY_EQUALS)
        .compareValues(
            "*", EnumSet.of(ValueComparisonOperator.EQ, ValueComparisonOperator.NE), JsonType.ARRAY)
        .capture(SUB_DOC_EQUALS)
        .compareValues(
            "*",
            EnumSet.of(ValueComparisonOperator.EQ, ValueComparisonOperator.NE),
            JsonType.SUB_DOC);
  }

  protected LogicalExpression resolve(CommandContext commandContext, T command) {
    // verify if filter fields are in deny list or not in allow list
    if (commandContext != null && command.filterClause() != null) {
      command.filterClause().validate(commandContext);
    }
    LogicalExpression filter = matchRules.apply(commandContext, command);
    if (filter.getTotalComparisonExpressionCount() > operationsConfig.maxFilterObjectProperties()) {
      throw new JsonApiException(
          ErrorCode.FILTER_FIELDS_LIMIT_VIOLATION,
          String.format(
              "%s: filter has %d fields, exceeds maximum allowed %s",
              ErrorCode.FILTER_FIELDS_LIMIT_VIOLATION.getMessage(),
              filter.getTotalComparisonExpressionCount(),
              operationsConfig.maxFilterObjectProperties()));
    }
    return filter;
  }

  public static List<DBFilterBase> findById(CaptureExpression captureExpression) {
    List<DBFilterBase> filters = new ArrayList<>();
    for (FilterOperation<?> filterOperation : captureExpression.filterOperations()) {
      if (captureExpression.marker() == ID_GROUP) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.EQ, (DocumentId) filterOperation.operand().value()));
      }
      if (captureExpression.marker() == ID_GROUP_IN) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.IN,
                (List<DocumentId>) filterOperation.operand().value()));
      }
    }
    return filters;
  }

  public static List<DBFilterBase> findNoFilter(CaptureExpression captureExpression) {
    return List.of();
  }

  public static List<DBFilterBase> findDynamic(CaptureExpression captureExpression) {
    List<DBFilterBase> filters = new ArrayList<>();
    for (FilterOperation<?> filterOperation : captureExpression.filterOperations()) {
      if (captureExpression.marker() == ID_GROUP) {
        switch ((ValueComparisonOperator) filterOperation.operator()) {
          case EQ:
            filters.add(
                new DBFilterBase.IDFilter(
                    DBFilterBase.IDFilter.Operator.EQ,
                    (DocumentId) filterOperation.operand().value()));
            break;
          case NE:
            filters.add(
                new DBFilterBase.IDFilter(
                    DBFilterBase.IDFilter.Operator.NE,
                    (DocumentId) filterOperation.operand().value()));
            break;
          default:
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
                String.format(
                    "Unsupported filter operator %s ", filterOperation.operator().getOperator()));
        }
      }
      if (captureExpression.marker() == ID_GROUP_IN) {
        switch ((ValueComparisonOperator) filterOperation.operator()) {
          case IN:
            filters.add(
                new DBFilterBase.IDFilter(
                    DBFilterBase.IDFilter.Operator.IN,
                    (List<DocumentId>) filterOperation.operand().value()));
            break;
          case NIN:
            filters.add(
                new DBFilterBase.InFilter(
                    getInFilterBaseOperator(filterOperation.operator()),
                    captureExpression.path(),
                    (List<Object>) filterOperation.operand().value()));
            break;
          default:
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
                String.format(
                    "Unsupported filter operator %s ", filterOperation.operator().getOperator()));
        }
      }

      if (captureExpression.marker() == ID_GROUP_RANGE) {
        final DocumentId value = (DocumentId) filterOperation.operand().value();
        if (value.value() instanceof BigDecimal bdv) {
          filters.add(
              new DBFilterBase.NumberFilter(
                  DocumentConstants.Fields.DOC_ID,
                  getMapFilterBaseOperator(filterOperation.operator()),
                  bdv));
        }
        if (value.value() instanceof Map) {
          filters.add(
              new DBFilterBase.DateFilter(
                  DocumentConstants.Fields.DOC_ID,
                  getMapFilterBaseOperator(filterOperation.operator()),
                  JsonUtil.createDateFromDocumentId(value)));
        }
      }

      if (captureExpression.marker() == DYNAMIC_GROUP_IN) {
        filters.add(
            new DBFilterBase.InFilter(
                getInFilterBaseOperator(filterOperation.operator()),
                captureExpression.path(),
                (List<Object>) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_TEXT_GROUP) {
        filters.add(
            new DBFilterBase.TextFilter(
                captureExpression.path(),
                getMapFilterBaseOperator(filterOperation.operator()),
                (String) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_BOOL_GROUP) {
        filters.add(
            new DBFilterBase.BoolFilter(
                captureExpression.path(),
                getMapFilterBaseOperator(filterOperation.operator()),
                (Boolean) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_NUMBER_GROUP) {
        filters.add(
            new DBFilterBase.NumberFilter(
                captureExpression.path(),
                getMapFilterBaseOperator(filterOperation.operator()),
                (BigDecimal) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_NULL_GROUP) {
        filters.add(
            new DBFilterBase.IsNullFilter(
                captureExpression.path(), getSetFilterBaseOperator(filterOperation.operator())));
      }

      if (captureExpression.marker() == DYNAMIC_DATE_GROUP) {
        filters.add(
            new DBFilterBase.DateFilter(
                captureExpression.path(),
                getMapFilterBaseOperator(filterOperation.operator()),
                (Date) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == EXISTS_GROUP) {
        Boolean bool = (Boolean) filterOperation.operand().value();
        filters.add(new DBFilterBase.ExistsFilter(captureExpression.path(), bool));
      }

      if (captureExpression.marker() == ALL_GROUP) {
        List<Object> arrayValue = (List<Object>) filterOperation.operand().value();
        filters.add(new DBFilterBase.AllFilter(captureExpression.path(), arrayValue, false));
      }

      if (captureExpression.marker() == NOT_ANY_GROUP) {
        List<Object> arrayValue = (List<Object>) filterOperation.operand().value();
        filters.add(new DBFilterBase.AllFilter(captureExpression.path(), arrayValue, true));
      }

      if (captureExpression.marker() == SIZE_GROUP) {
        BigDecimal bigDecimal = (BigDecimal) filterOperation.operand().value();
        // Flipping size operator will multiply the value by -1
        // Negative means check array_size[?] != ?
        int size = bigDecimal.intValue();
        DBFilterBase.MapFilterBase.Operator operator;
        if (size > 0) {
          operator = DBFilterBase.MapFilterBase.Operator.MAP_EQUALS;
        } else {
          operator = DBFilterBase.MapFilterBase.Operator.MAP_NOT_EQUALS;
        }
        filters.add(
            new DBFilterBase.SizeFilter(captureExpression.path(), operator, Math.abs(size)));
      }

      if (captureExpression.marker() == ARRAY_EQUALS) {
        filters.add(
            new DBFilterBase.ArrayEqualsFilter(
                new DocValueHasher(),
                captureExpression.path(),
                (List<Object>) filterOperation.operand().value(),
                filterOperation.operator().equals(ValueComparisonOperator.EQ)
                    ? DBFilterBase.MapFilterBase.Operator.MAP_EQUALS
                    : DBFilterBase.MapFilterBase.Operator.MAP_NOT_EQUALS));
      }

      if (captureExpression.marker() == SUB_DOC_EQUALS) {
        filters.add(
            new DBFilterBase.SubDocEqualsFilter(
                new DocValueHasher(),
                captureExpression.path(),
                (Map<String, Object>) filterOperation.operand().value(),
                filterOperation.operator().equals(ValueComparisonOperator.EQ)
                    ? DBFilterBase.MapFilterBase.Operator.MAP_EQUALS
                    : DBFilterBase.MapFilterBase.Operator.MAP_NOT_EQUALS));
      }
    }

    return filters;
  }

  private static DBFilterBase.MapFilterBase.Operator getMapFilterBaseOperator(
      FilterOperator filterOperator) {
    switch ((ValueComparisonOperator) filterOperator) {
      case EQ:
        return DBFilterBase.MapFilterBase.Operator.EQ;
      case NE:
        return DBFilterBase.MapFilterBase.Operator.NE;
      case GT:
        return DBFilterBase.MapFilterBase.Operator.GT;
      case GTE:
        return DBFilterBase.MapFilterBase.Operator.GTE;
      case LT:
        return DBFilterBase.MapFilterBase.Operator.LT;
      case LTE:
        return DBFilterBase.MapFilterBase.Operator.LTE;
      default:
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
            String.format("Unsupported filter operator %s ", filterOperator.getOperator()));
    }
  }

  private static DBFilterBase.InFilter.Operator getInFilterBaseOperator(
      FilterOperator filterOperator) {
    switch ((ValueComparisonOperator) filterOperator) {
      case IN:
        return DBFilterBase.InFilter.Operator.IN;
      case NIN:
        return DBFilterBase.InFilter.Operator.NIN;
      default:
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
            String.format("Unsupported filter operator %s ", filterOperator.getOperator()));
    }
  }

  private static DBFilterBase.SetFilterBase.Operator getSetFilterBaseOperator(
      FilterOperator filterOperator) {
    switch ((ValueComparisonOperator) filterOperator) {
      case EQ:
        return DBFilterBase.SetFilterBase.Operator.CONTAINS;
      case NE:
        return DBFilterBase.SetFilterBase.Operator.NOT_CONTAINS;
      default:
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
            String.format("Unsupported filter operator %s ", filterOperator.getOperator()));
    }
  }
}
