package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.*;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.math.BigDecimal;
import java.util.*;
import java.util.function.BiConsumer;

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
            EnumSet.of(
                ValueComparisonOperator.EQ,
                ValueComparisonOperator.NE,
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.STRING)
        .capture(DYNAMIC_BOOL_GROUP)
        .compareValues(
            "*",
            EnumSet.of(
                ValueComparisonOperator.EQ,
                ValueComparisonOperator.NE,
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
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

  public static DBLogicalExpression findById(
      DBLogicalExpression currentDbLogicalExpression, CaptureGroups currentCaptureGroups) {

    BiConsumer<CaptureGroups, DBLogicalExpression> consumer =
        (captureGroups, dbLogicalExpression) -> {
          // convert captureGroup to DBFilter

          captureGroups
              .getGroupIfPresent(ID_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<DocumentId> idGroup = (CaptureGroup<DocumentId>) captureGroup;
                    idGroup.consumeAllCaptures(
                        expression ->
                            dbLogicalExpression.addDBFilter(
                                new IDCollectionFilter(
                                    IDCollectionFilter.Operator.EQ,
                                    (DocumentId) expression.value())));
                  });

          captureGroups
              .getGroupIfPresent(ID_GROUP_IN)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<DocumentId> idInGroup = (CaptureGroup<DocumentId>) captureGroup;
                    idInGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addDBFilter(
                              new IDCollectionFilter(
                                  IDCollectionFilter.Operator.IN,
                                  (List<DocumentId>) expression.value()));
                        });
                  });
        };

    currentCaptureGroups.consumeAll(currentDbLogicalExpression, consumer);

    return currentDbLogicalExpression;
  }

  public static DBLogicalExpression findNoFilter(
      DBLogicalExpression currentDbLogicalExpression, CaptureGroups currentCaptureGroups) {
    return currentDbLogicalExpression;
  }

  public static DBLogicalExpression findDynamic(
      DBLogicalExpression currentDbLogicalExpression, CaptureGroups currentCaptureGroups) {

    BiConsumer<CaptureGroups, DBLogicalExpression> consumer =
        (captureGroups, dbLogicalExpression) -> {
          captureGroups
              .getGroupIfPresent(ID_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<DocumentId> idGroup = (CaptureGroup<DocumentId>) captureGroup;
                    idGroup.consumeAllCaptures(
                        expression -> {
                          switch ((ValueComparisonOperator) expression.operator()) {
                            case EQ:
                              dbLogicalExpression.addDBFilter(
                                  new IDCollectionFilter(
                                      IDCollectionFilter.Operator.EQ, expression.value()));
                              break;
                            case NE:
                              dbLogicalExpression.addDBFilter(
                                  new IDCollectionFilter(
                                      IDCollectionFilter.Operator.NE, expression.value()));
                              break;
                            default:
                              throw ErrorCodeV1.UNSUPPORTED_FILTER_OPERATION.toApiException(
                                  "%s", expression.operator());
                          }
                        });
                  });

          captureGroups
              .getGroupIfPresent(ID_GROUP_IN)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> idInGroup = (CaptureGroup<Object>) captureGroup;
                    idInGroup.consumeAllCaptures(
                        expression -> {
                          switch ((ValueComparisonOperator) expression.operator()) {
                            case IN:
                              dbLogicalExpression.addDBFilter(
                                  new IDCollectionFilter(
                                      IDCollectionFilter.Operator.IN,
                                      (List<DocumentId>) expression.value()));
                              break;
                            case NIN:
                              dbLogicalExpression.addDBFilter(
                                  new InCollectionFilter(
                                      getInFilterBaseOperator(expression.operator()),
                                      expression.path(),
                                      (List<Object>) expression.value()));
                              break;
                            default:
                              throw ErrorCodeV1.UNSUPPORTED_FILTER_OPERATION.toApiException(
                                  "%s", expression.operator());
                          }
                        });
                  });

          captureGroups
              .getGroupIfPresent(ID_GROUP_RANGE)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<DocumentId> idRangeGroup = (CaptureGroup<DocumentId>) captureGroup;
                    idRangeGroup.consumeAllCaptures(
                        expression -> {
                          final DocumentId value = (DocumentId) expression.value();
                          if (value.value() instanceof BigDecimal bdv) {
                            dbLogicalExpression.addDBFilter(
                                new NumberCollectionFilter(
                                    DocumentConstants.Fields.DOC_ID,
                                    getMapFilterBaseOperator(expression.operator()),
                                    bdv));
                          }
                          if (value.value() instanceof Map) {
                            dbLogicalExpression.addDBFilter(
                                new DateCollectionFilter(
                                    DocumentConstants.Fields.DOC_ID,
                                    getMapFilterBaseOperator(expression.operator()),
                                    JsonUtil.createDateFromDocumentId(value)));
                          }
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_GROUP_IN)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> dynamicInGroup = (CaptureGroup<Object>) captureGroup;
                    dynamicInGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addDBFilter(
                              new InCollectionFilter(
                                  getInFilterBaseOperator(expression.operator()),
                                  expression.path(),
                                  (List<Object>) expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_TEXT_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<String> dynamicTextGroup = (CaptureGroup<String>) captureGroup;
                    dynamicTextGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addDBFilter(
                              new TextCollectionFilter(
                                  expression.path(),
                                  getMapFilterBaseOperator(expression.operator()),
                                  expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_BOOL_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Boolean> dynamicBoolGroup = (CaptureGroup<Boolean>) captureGroup;
                    dynamicBoolGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addDBFilter(
                              new BoolCollectionFilter(
                                  expression.path(),
                                  getMapFilterBaseOperator(expression.operator()),
                                  expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_NUMBER_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<BigDecimal> numberGroup = (CaptureGroup<BigDecimal>) captureGroup;
                    numberGroup.consumeAllCaptures(
                        expression ->
                            dbLogicalExpression.addDBFilter(
                                new NumberCollectionFilter(
                                    expression.path(),
                                    getMapFilterBaseOperator(expression.operator()),
                                    expression.value())));
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_NULL_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> nullGroup = (CaptureGroup<Object>) captureGroup;
                    nullGroup.consumeAllCaptures(
                        expression ->
                            dbLogicalExpression.addDBFilter(
                                new IsNullCollectionFilter(
                                    expression.path(),
                                    getSetFilterBaseOperator(expression.operator()))));
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_DATE_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Date> dateGroup = (CaptureGroup<Date>) captureGroup;
                    dateGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addDBFilter(
                              new DateCollectionFilter(
                                  expression.path(),
                                  getMapFilterBaseOperator(expression.operator()),
                                  (Date) expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(EXISTS_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Boolean> existsGroup = (CaptureGroup<Boolean>) captureGroup;
                    existsGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addDBFilter(
                              new ExistsCollectionFilter(expression.path(), expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(ALL_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> allGroup = (CaptureGroup<Object>) captureGroup;
                    allGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addDBFilter(
                              new AllCollectionFilter(
                                  expression.path(), (List<Object>) expression.value(), false));
                        });
                  });

          captureGroups
              .getGroupIfPresent(NOT_ANY_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> notAnyGroup = (CaptureGroup<Object>) captureGroup;
                    notAnyGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addDBFilter(
                              new AllCollectionFilter(
                                  expression.path(), (List<Object>) expression.value(), true));
                        });
                  });

          captureGroups
              .getGroupIfPresent(SIZE_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> sizeGroup = (CaptureGroup<Object>) captureGroup;
                    sizeGroup.consumeAllCaptures(
                        expression -> {
                          if (expression.value() instanceof Boolean) {
                            // This is the special case, e.g. {"$not":{"ages":{"$size":0}}}
                            dbLogicalExpression.addDBFilter(
                                new SizeCollectionFilter(
                                    expression.path(),
                                    MapCollectionFilter.Operator.MAP_NOT_EQUALS,
                                    0));
                          } else {
                            BigDecimal bigDecimal = (BigDecimal) expression.value();
                            // Flipping size operator will multiply the value by -1
                            // Negative means check array_size[?] != ?
                            int size = bigDecimal.intValue();
                            MapCollectionFilter.Operator operator;
                            if (size >= 0) {
                              operator = MapCollectionFilter.Operator.MAP_EQUALS;
                            } else {
                              operator = MapCollectionFilter.Operator.MAP_NOT_EQUALS;
                            }
                            dbLogicalExpression.addDBFilter(
                                new SizeCollectionFilter(
                                    expression.path(), operator, Math.abs(size)));
                          }
                        });
                  });

          captureGroups
              .getGroupIfPresent(ARRAY_EQUALS)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> arrayEqualsGroup = (CaptureGroup<Object>) captureGroup;
                    arrayEqualsGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addDBFilter(
                              new ArrayEqualsCollectionFilter(
                                  new DocValueHasher(),
                                  expression.path(),
                                  (List<Object>) expression.value(),
                                  expression.operator().equals(ValueComparisonOperator.EQ)
                                      ? MapCollectionFilter.Operator.MAP_EQUALS
                                      : MapCollectionFilter.Operator.MAP_NOT_EQUALS));
                        });
                  });

          captureGroups
              .getGroupIfPresent(SUB_DOC_EQUALS)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> subDocEqualsGroup = (CaptureGroup<Object>) captureGroup;
                    subDocEqualsGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addDBFilter(
                              new SubDocEqualsCollectionFilter(
                                  new DocValueHasher(),
                                  expression.path(),
                                  (Map<String, Object>) expression.value(),
                                  expression.operator().equals(ValueComparisonOperator.EQ)
                                      ? MapCollectionFilter.Operator.MAP_EQUALS
                                      : MapCollectionFilter.Operator.MAP_NOT_EQUALS));
                        });
                  });
        };

    currentCaptureGroups.consumeAll(currentDbLogicalExpression, consumer);
    return currentDbLogicalExpression;
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
        throw ErrorCodeV1.UNSUPPORTED_FILTER_OPERATION.toApiException(
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
        throw ErrorCodeV1.UNSUPPORTED_FILTER_OPERATION.toApiException(
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
        throw ErrorCodeV1.UNSUPPORTED_FILTER_OPERATION.toApiException(
            "%s", filterOperator.getOperator());
    }
  }
}
