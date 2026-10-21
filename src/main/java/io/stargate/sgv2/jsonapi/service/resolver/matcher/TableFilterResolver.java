package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ArrayComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.CollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.*;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.BinaryTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.OpenSearchTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiOpenSearchIndex;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * POC for a filter clause resolver that can handle the filter clause for a table.
 *
 * @param <CmdT>
 */
public class TableFilterResolver<CmdT extends Command & Filterable>
    extends FilterResolver<CmdT, TableSchemaObject> {

  private static final Object DYNAMIC_DOCID_GROUP = new Object();
  private static final Object DYNAMIC_TEXT_GROUP = new Object();
  private static final Object DYNAMIC_NUMBER_GROUP = new Object();
  private static final Object DYNAMIC_BOOL_GROUP = new Object();
  private static final Object DYNAMIC_EJSON_GROUP = new Object();
  private static final Object DYNAMIC_GROUP_IN = new Object();
  private static final Object DYNAMIC_MAP_SET_LIST_GROUP = new Object();
  private static final Object DYNAMIC_MATCH_GROUP = new Object();
  private static final Object DYNAMIC_SUB_DOC_GROUP = new Object();
  // For $openSearch (OpenSearch) — must come before GREEDY rule so it is matched first
  private static final Object SEARCH_GROUP = new Object();

  public TableFilterResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }

  /**
   * Resolves the filter and, when it contains a {@code $openSearch} (OpenSearch) filter, validates
   * it and fills in the SAI index name from the table schema. Doing this here means every table
   * command that accepts a filter (find, findOne, updateOne, deleteOne, deleteMany, sort
   * resolution, ...) handles {@code $openSearch} the same way.
   */
  @Override
  public WithWarnings<DBLogicalExpression> resolve(
      CommandContext<TableSchemaObject> commandContext, CmdT command) {
    var resolved = super.resolve(commandContext, command);
    resolveOpenSearchFilter(commandContext.schemaObject(), resolved.target());
    return resolved;
  }

  /**
   * Detects {@link OpenSearchTableFilter} instances in the resolved logical expression and:
   *
   * <ol>
   *   <li>Validates that {@code $openSearch} is the only filter (throws {@code
   *       INVALID_OPEN_SEARCH_FILTER} otherwise)
   *   <li>Looks up the {@link ApiOpenSearchIndex} from the table schema (throws {@code
   *       OPEN_SEARCH_NOT_ENABLED_FOR_TABLE} if none exists)
   *   <li>Populates the SAI index name on each pending {@link OpenSearchTableFilter}
   * </ol>
   */
  private static void resolveOpenSearchFilter(
      TableSchemaObject schemaObject, DBLogicalExpression dbLogicalExpression) {

    List<OpenSearchTableFilter> osFilters = new ArrayList<>();
    dbLogicalExpression.visitAllFilters(OpenSearchTableFilter.class, osFilters::add);

    if (osFilters.isEmpty()) {
      return; // no $openSearch in this request — nothing to do
    }

    // $openSearch must be the only filter in the expression tree
    long otherFilters =
        dbLogicalExpression.filters().stream()
            .filter(f -> !(f instanceof OpenSearchTableFilter))
            .count();
    if (otherFilters > 0 || !dbLogicalExpression.subExpressions().isEmpty()) {
      throw FilterException.Code.INVALID_OPEN_SEARCH_FILTER.get();
    }

    ApiOpenSearchIndex osIndex =
        schemaObject.apiTableDef().indexes().allIndexes().stream()
            .filter(idx -> idx.indexType() == ApiIndexType.OPEN_SEARCH)
            .filter(ApiOpenSearchIndex.class::isInstance)
            .map(ApiOpenSearchIndex.class::cast)
            .findFirst()
            .orElseThrow(
                () ->
                    SchemaException.Code.OPEN_SEARCH_NOT_ENABLED_FOR_TABLE.get(
                        errVars(schemaObject)));

    String saiIndexName = osIndex.saiIndexName();
    osFilters.forEach(f -> f.setSaiIndexName(saiIndexName));
  }

  @Override
  protected FilterMatchRules<CmdT> buildMatchRules() {
    var matchRules = new FilterMatchRules<CmdT>();

    matchRules.addMatchRule(TableFilterResolver::findNoFilter, FilterMatcher.MatchStrategy.EMPTY);

    // Dedicated rule for $openSearch placed BEFORE the GREEDY rule so that a $openSearch filter is
    // matched here and never stolen by the wildcard ("*") captures. GREEDY is used because the
    // two captures cover the two legal value shapes (String and sub-document); only one will
    // fire and GREEDY tolerates unmatched captures. $openSearch is always the sole filter
    // (enforced by TableFilterClauseBuilder), so unmatchedComparisonExpressionCount will
    // reach 0 and the rule succeeds.
    matchRules
        .addMatchRule(TableFilterResolver::findDynamic, FilterMatcher.MatchStrategy.GREEDY)
        .matcher()
        .capture(SEARCH_GROUP)
        .compareValues(
            DocumentConstants.Fields.OPEN_SEARCH_CONTENT_FIELD,
            EnumSet.of(ValueComparisonOperator.EQ),
            JsonType.STRING)
        .capture(SEARCH_GROUP)
        .compareValues(
            DocumentConstants.Fields.OPEN_SEARCH_CONTENT_FIELD,
            EnumSet.of(ValueComparisonOperator.EQ),
            JsonType.SUB_DOC);

    matchRules
        .addMatchRule(TableFilterResolver::findDynamic, FilterMatcher.MatchStrategy.GREEDY)
        .matcher()
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
        // For now only EJSON support is for binary data
        .capture(DYNAMIC_EJSON_GROUP)
        .compareValues(
            "*",
            EnumSet.of(ValueComparisonOperator.EQ, ValueComparisonOperator.NE),
            JsonType.EJSON_WRAPPER)
        // Although Tables does not have special handling for _id, our FilterClauseDeserializer
        // does, so we need to capture it here.
        .capture(DYNAMIC_DOCID_GROUP)
        .compareValues(
            "_id",
            EnumSet.of(
                ValueComparisonOperator.EQ,
                //                ValueComparisonOperator.NE, // TODO: not sure this is supported
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
        // Four filter operators are allowed for map/set/list, value type must be ARRAY.
        .capture(DYNAMIC_MAP_SET_LIST_GROUP)
        .compareMapSetListFilterValues(
            "*",
            Set.of(
                ValueComparisonOperator.IN,
                ValueComparisonOperator.NIN,
                ArrayComparisonOperator.ALL,
                // NOTANY is not public exposed in API, only be negated by ALL.
                ArrayComparisonOperator.NOTANY),
            JsonType.ARRAY)
        .capture(DYNAMIC_MATCH_GROUP) // For $match operator
        .compareValues("*", EnumSet.of(ValueComparisonOperator.MATCH), JsonType.STRING)
        // 04-Mar-2026, tatu: [data-api#2275] Capture SUB_DOC (JSON Object) so they don't cause a
        // generic server error; findDynamic handler will throw a descriptive FilterException
        // instead.
        .capture(DYNAMIC_SUB_DOC_GROUP)
        .compareValues(
            "*",
            EnumSet.of(
                ValueComparisonOperator.EQ,
                ValueComparisonOperator.NE,
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.SUB_DOC);
    return matchRules;
  }

  public static DBLogicalExpression findNoFilter(
      DBLogicalExpression dbLogicalExpression, CaptureGroups currentCaptureGroups) {
    return dbLogicalExpression;
  }

  public static DBLogicalExpression findDynamic(
      DBLogicalExpression currentDBLogicalExpression, CaptureGroups currentCaptureGroups) {

    // TODO: How do we know what the CmdT of the JsonLiteral<CmdT> from .value() is ?
    BiConsumer<CaptureGroups, DBLogicalExpression> consumer =
        (captureGroups, dbLogicalExpression) -> {
          captureGroups
              .getGroupIfPresent(DYNAMIC_TEXT_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<String> dynamicTextGroup = (CaptureGroup<String>) captureGroup;
                    dynamicTextGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addFilter(
                              new TextTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_NUMBER_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<BigDecimal> dynamicNumberGroup =
                        (CaptureGroup<BigDecimal>) captureGroup;
                    dynamicNumberGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addFilter(
                              new NumberTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  (BigDecimal) expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_BOOL_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Boolean> dynamicNumberGroup = (CaptureGroup<Boolean>) captureGroup;
                    dynamicNumberGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addFilter(
                              new BooleanTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  (Boolean) expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_EJSON_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> dynamicNumberGroup = (CaptureGroup<Object>) captureGroup;
                    dynamicNumberGroup.consumeAllCaptures(
                        expression -> {
                          byte[] bytes = (byte[]) expression.value();
                          EJSONWrapper boundValue = EJSONWrapper.binaryWrapper(bytes);
                          dbLogicalExpression.addFilter(
                              new BinaryTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  boundValue));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_DOCID_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> dynamicDocIDGroup = (CaptureGroup<Object>) captureGroup;
                    dynamicDocIDGroup.consumeAllCaptures(
                        expression -> {
                          Object rhsValue = ((DocumentId) expression.value()).value();
                          if (rhsValue instanceof String) {
                            dbLogicalExpression.addFilter(
                                new TextTableFilter(
                                    expression.path(),
                                    NativeTypeTableFilter.Operator.from(
                                        (ValueComparisonOperator) expression.operator()),
                                    (String) rhsValue));
                          } else if (rhsValue instanceof Number) {
                            dbLogicalExpression.addFilter(
                                new NumberTableFilter(
                                    expression.path(),
                                    NativeTypeTableFilter.Operator.from(
                                        (ValueComparisonOperator) expression.operator()),
                                    (Number) rhsValue));
                          } else {
                            throw new UnsupportedOperationException(
                                "Unsupported DocumentId type: " + rhsValue.getClass().getName());
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
                          dbLogicalExpression.addFilter(
                              new InTableFilter(
                                  InTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  expression.path(),
                                  (List<Object>) expression.value()));
                        });
                  });

          // consume captures for map/set/list and convert to MapSetListTableFilter
          captureGroups
              .getGroupIfPresent(DYNAMIC_MAP_SET_LIST_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> mapSetListGroup = (CaptureGroup<Object>) captureGroup;
                    mapSetListGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addFilter(
                              new MapSetListTableFilter(
                                  MapSetListTableFilter.Operator.from(expression.operator()),
                                  expression.path(),
                                  (List<Object>) expression.value(),
                                  expression.filterComponent()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_MATCH_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<String> dynamicMatchGroup = (CaptureGroup<String>) captureGroup;
                    dynamicMatchGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addFilter(
                              new TextTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  expression.value()));
                        });
                  });

          // $openSearch (OpenSearch) — the value is the verbatim OpenSearch DSL (a sub-document
          // or occasionally a plain string). The OpenSearchTableFilter is constructed here;
          // the saiIndexName is resolved later in TableReadDBOperationBuilder after looking up
          // ApiOpenSearchIndex from the schema. We store a sentinel empty string for now; the
          // builder will replace it via a direct instanceof check.
          captureGroups
              .getGroupIfPresent(SEARCH_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> searchGroup = (CaptureGroup<Object>) captureGroup;
                    searchGroup.consumeAllCaptures(
                        expression -> {
                          // The filter builder hands us plain Java values (Map/List/String/
                          // BigDecimal/Boolean), not JsonNode, so convert back to JSON; a
                          // sub-document must stay a JSON object for HCD to parse the DSL.
                          JsonNode dslNode =
                              CollectionFilter.toJsonNode(
                                  JsonNodeFactory.instance, expression.value());
                          // saiIndexName is resolved later in TableReadDBOperationBuilder
                          dbLogicalExpression.addFilter(new OpenSearchTableFilter(dslNode));
                        });
                  });

          // 04-Mar-2026, tatu: [data-api#2275] Reject JSON Object (SUB_DOC) values: tables
          // do not support them as filter operands
          captureGroups
              .getGroupIfPresent(DYNAMIC_SUB_DOC_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> subDocGroup = (CaptureGroup<Object>) captureGroup;
                    subDocGroup.consumeAllCaptures(
                        expression -> {
                          throw FilterException.Code.FILTER_UNSUPPORTED_DATA_TYPE.get(
                              "message",
                              "JSON Object values are not supported as table column filter values, filter column '"
                                  + expression.path()
                                  + "'");
                        });
                  });
        };

    currentCaptureGroups.consumeAll(currentDBLogicalExpression, consumer);
    return currentDBLogicalExpression;
  }
}
