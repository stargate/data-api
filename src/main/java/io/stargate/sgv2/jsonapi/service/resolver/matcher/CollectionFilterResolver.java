package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.*;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.math.BigDecimal;
import java.util.*;

/**
 * A {@link FilterResolver} for resolving {@link FilterClause} against a {@link
 * CollectionSchemaObject}.
 *
 * <p>This understands how filter operations like `$size` work with Collections.
 *
 * <p>TIDY: a lot of methods in this class a public for testing, change this TIDY: fix the unchecked
 * casts, may need some interface changes
 */
public class CollectionFilterResolver<T extends Command & Filterable>
    extends FilterResolver<T, CollectionSchemaObject> {

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

  public CollectionFilterResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }

  @Override
  protected FilterMatchRules<T> buildMatchRules() {
    var matchRules = new FilterMatchRules<T>();

    matchRules.addMatchRule(
        CollectionFilterResolver::findNoFilter, FilterMatcher.MatchStrategy.EMPTY);

    matchRules
        .addMatchRule(CollectionFilterResolver::findById, FilterMatcher.MatchStrategy.STRICT)
        .matcher()
        .capture(ID_GROUP)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.EQ), JsonType.DOCUMENT_ID);

    matchRules
        .addMatchRule(CollectionFilterResolver::findById, FilterMatcher.MatchStrategy.STRICT)
        .matcher()
        .capture(ID_GROUP_IN)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY);

    matchRules
        .addMatchRule(CollectionFilterResolver::findDynamic, FilterMatcher.MatchStrategy.GREEDY)
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

    return matchRules;
  }

  private static List<DBFilterBase> findById(CaptureExpression captureExpression) {
    List<DBFilterBase> filters = new ArrayList<>();
    for (FilterOperation<?> filterOperation : captureExpression.filterOperations()) {
      if (captureExpression.marker() == ID_GROUP) {
        filters.add(
            new IDCollectionFilter(
                IDCollectionFilter.Operator.EQ, (DocumentId) filterOperation.operand().value()));
      }
      // TIDY: Resolve the unchecked cast (List<DocumentId>) below and in other places in this file
      if (captureExpression.marker() == ID_GROUP_IN) {
        filters.add(
            new IDCollectionFilter(
                IDCollectionFilter.Operator.IN,
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
                new IDCollectionFilter(
                    IDCollectionFilter.Operator.EQ,
                    (DocumentId) filterOperation.operand().value()));
            break;
          case NE:
            filters.add(
                new IDCollectionFilter(
                    IDCollectionFilter.Operator.NE,
                    (DocumentId) filterOperation.operand().value()));
            break;
          default:
            throw ErrorCode.UNSUPPORTED_FILTER_OPERATION.toApiException(
                "%s", filterOperation.operator().getOperator());
        }
      }
      if (captureExpression.marker() == ID_GROUP_IN) {
        switch ((ValueComparisonOperator) filterOperation.operator()) {
          case IN:
            filters.add(
                new IDCollectionFilter(
                    IDCollectionFilter.Operator.IN,
                    (List<DocumentId>) filterOperation.operand().value()));
            break;
          case NIN:
            filters.add(
                new InCollectionFilter(
                    getInFilterBaseOperator(filterOperation.operator()),
                    captureExpression.path(),
                    (List<Object>) filterOperation.operand().value()));
            break;
          default:
            throw ErrorCode.UNSUPPORTED_FILTER_OPERATION.toApiException(
                "%s", filterOperation.operator().getOperator());
        }
      }

      if (captureExpression.marker() == ID_GROUP_RANGE) {
        final DocumentId value = (DocumentId) filterOperation.operand().value();
        if (value.value() instanceof BigDecimal bdv) {
          filters.add(
              new NumberCollectionFilter(
                  DocumentConstants.Fields.DOC_ID,
                  getMapFilterBaseOperator(filterOperation.operator()),
                  bdv));
        }
        if (value.value() instanceof Map) {
          filters.add(
              new DateCollectionFilter(
                  DocumentConstants.Fields.DOC_ID,
                  getMapFilterBaseOperator(filterOperation.operator()),
                  JsonUtil.createDateFromDocumentId(value)));
        }
      }

      if (captureExpression.marker() == DYNAMIC_GROUP_IN) {
        filters.add(
            new InCollectionFilter(
                getInFilterBaseOperator(filterOperation.operator()),
                captureExpression.path(),
                (List<Object>) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_TEXT_GROUP) {
        filters.add(
            new TextCollectionFilter(
                captureExpression.path(),
                getMapFilterBaseOperator(filterOperation.operator()),
                (String) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_BOOL_GROUP) {
        filters.add(
            new BoolCollectionFilter(
                captureExpression.path(),
                getMapFilterBaseOperator(filterOperation.operator()),
                (Boolean) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_NUMBER_GROUP) {
        filters.add(
            new NumberCollectionFilter(
                captureExpression.path(),
                getMapFilterBaseOperator(filterOperation.operator()),
                (BigDecimal) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_NULL_GROUP) {
        filters.add(
            new IsNullCollectionFilter(
                captureExpression.path(), getSetFilterBaseOperator(filterOperation.operator())));
      }

      if (captureExpression.marker() == DYNAMIC_DATE_GROUP) {
        filters.add(
            new DateCollectionFilter(
                captureExpression.path(),
                getMapFilterBaseOperator(filterOperation.operator()),
                (Date) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == EXISTS_GROUP) {
        Boolean bool = (Boolean) filterOperation.operand().value();
        filters.add(new ExistsCollectionFilter(captureExpression.path(), bool));
      }

      if (captureExpression.marker() == ALL_GROUP) {
        List<Object> arrayValue = (List<Object>) filterOperation.operand().value();
        filters.add(new AllCollectionFilter(captureExpression.path(), arrayValue, false));
      }

      if (captureExpression.marker() == NOT_ANY_GROUP) {
        List<Object> arrayValue = (List<Object>) filterOperation.operand().value();
        filters.add(new AllCollectionFilter(captureExpression.path(), arrayValue, true));
      }

      if (captureExpression.marker() == SIZE_GROUP) {
        if (filterOperation.operand().value() instanceof Boolean) {
          // This is the special case, e.g. {"$not":{"ages":{"$size":0}}}
          filters.add(
              new SizeCollectionFilter(
                  captureExpression.path(), MapCollectionFilter.Operator.MAP_NOT_EQUALS, 0));
        } else {
          BigDecimal bigDecimal = (BigDecimal) filterOperation.operand().value();
          // Flipping size operator will multiply the value by -1
          // Negative means check array_size[?] != ?
          int size = bigDecimal.intValue();
          MapCollectionFilter.Operator operator;
          if (size >= 0) {
            operator = MapCollectionFilter.Operator.MAP_EQUALS;
          } else {
            operator = MapCollectionFilter.Operator.MAP_NOT_EQUALS;
          }
          filters.add(new SizeCollectionFilter(captureExpression.path(), operator, Math.abs(size)));
        }
      }

      if (captureExpression.marker() == ARRAY_EQUALS) {
        filters.add(
            new ArrayEqualsCollectionFilter(
                new DocValueHasher(),
                captureExpression.path(),
                (List<Object>) filterOperation.operand().value(),
                filterOperation.operator().equals(ValueComparisonOperator.EQ)
                    ? MapCollectionFilter.Operator.MAP_EQUALS
                    : MapCollectionFilter.Operator.MAP_NOT_EQUALS));
      }

      if (captureExpression.marker() == SUB_DOC_EQUALS) {
        filters.add(
            new SubDocEqualsCollectionFilter(
                new DocValueHasher(),
                captureExpression.path(),
                (Map<String, Object>) filterOperation.operand().value(),
                filterOperation.operator().equals(ValueComparisonOperator.EQ)
                    ? MapCollectionFilter.Operator.MAP_EQUALS
                    : MapCollectionFilter.Operator.MAP_NOT_EQUALS));
      }
    }

    return filters;
  }

  // TIDY move these to the MapCollectionFilter etc enums
  private static MapCollectionFilter.Operator getMapFilterBaseOperator(
      FilterOperator filterOperator) {
    switch ((ValueComparisonOperator) filterOperator) {
      case EQ:
        return MapCollectionFilter.Operator.EQ;
      case NE:
        return MapCollectionFilter.Operator.NE;
      case GT:
        return MapCollectionFilter.Operator.GT;
      case GTE:
        return MapCollectionFilter.Operator.GTE;
      case LT:
        return MapCollectionFilter.Operator.LT;
      case LTE:
        return MapCollectionFilter.Operator.LTE;
      default:
        throw ErrorCode.UNSUPPORTED_FILTER_OPERATION.toApiException(
            "%s", filterOperator.getOperator());
    }
  }

  private static InCollectionFilter.Operator getInFilterBaseOperator(
      FilterOperator filterOperator) {
    switch ((ValueComparisonOperator) filterOperator) {
      case IN:
        return InCollectionFilter.Operator.IN;
      case NIN:
        return InCollectionFilter.Operator.NIN;
      default:
        throw ErrorCode.UNSUPPORTED_FILTER_OPERATION.toApiException(
            "%s", filterOperator.getOperator());
    }
  }

  private static SetCollectionFilter.Operator getSetFilterBaseOperator(
      FilterOperator filterOperator) {
    switch ((ValueComparisonOperator) filterOperator) {
      case EQ:
        return SetCollectionFilter.Operator.CONTAINS;
      case NE:
        return SetCollectionFilter.Operator.NOT_CONTAINS;
      default:
        throw ErrorCode.UNSUPPORTED_FILTER_OPERATION.toApiException(
            "%s", filterOperator.getOperator());
    }
  }
}
