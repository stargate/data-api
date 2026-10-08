package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.LegMode;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.config.IntConfigWithBounds;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Decides what a findAndRerank command will do: which legs to read and how many documents each one
 * reads, the reranking service, the rerank query and passage, and how many documents to return.
 *
 * <p>The planner validates the request and throws if it cannot be run. It only uses its inputs, it
 * does not call the database or any model and does not read the command context, so it can be
 * tested without Quarkus.
 *
 * <p>The {@link LegMode} is what the user asked for, the legs are what will run. They can be
 * different, for example <code>{"$hybrid": "cheese"}</code> skips the lexical leg when the
 * collection has no lexical index.
 */
public class FindAndRerankPlanner {

  private FindAndRerankPlanner() {
    // prevent instantiation
  }

  /**
   * Plans the findAndRerank command.
   *
   * @param legMode The mode from the parsed sort.
   * @param explicitLexical True if the user wrote the <code>$lexical</code> key in the <code>
   *     $hybrid</code> sort, even when its value is null or blank.
   * @param sort The parsed sort.
   * @param options The command options, may be <code>null</code>.
   * @param schemaObject The collection the command runs against.
   * @param operationsConfig Config for the bounds of <code>hybridLimits</code> and the default
   *     limit.
   * @param rerankingProvidersConfig Config used to validate the reranking service.
   * @return The plan, it always has at least one leg.
   */
  public static FindAndRerankPlan plan(
      LegMode legMode,
      boolean explicitLexical,
      FindAndRerankSort sort,
      FindAndRerankCommand.Options options,
      CollectionSchemaObject schemaObject,
      OperationsConfig operationsConfig,
      RerankingProvidersConfig rerankingProvidersConfig) {

    Objects.requireNonNull(legMode, "legMode must not be null");
    Objects.requireNonNull(sort, "sort must not be null");
    Objects.requireNonNull(schemaObject, "schemaObject must not be null");
    Objects.requireNonNull(operationsConfig, "operationsConfig must not be null");
    Objects.requireNonNull(rerankingProvidersConfig, "rerankingProvidersConfig must not be null");

    // NOTE: the order of the steps is the order of the errors the API returns, the first failing
    // check wins: sort support, hybridLimits, rerank service, rerank query, passage field, then the
    // mode and $hybrid value checks.
    checkSortSupported(explicitLexical, sort, schemaObject);
    checkHybridLimits(options, operationsConfig);
    var rerankServiceDef = resolveRerankServiceDef(options, schemaObject, rerankingProvidersConfig);

    // only HYBRID is supported for now, the other modes have no legs and fail below
    List<RerankLeg> legs =
        switch (legMode) {
          case HYBRID -> hybridLegs(sort, options, schemaObject);
          case VECTORIZE, VECTOR, LEXICAL, FILTER -> List.of();
        };

    // only the vectorize leg has a default query, so the leg default is the vectorize query
    var rerankingQuery =
        RerankingQuery.create(
            getOrDefault(options, FindAndRerankCommand.Options::rerankQuery, null),
            firstLegDefault(legs, RerankLeg::defaultRerankQuery));
    var passageLocator = PathMatchLocator.forPath(passageField(options, legs));

    // these come after the rerank query and passage checks, so a request with no valued sort (e.g.
    // {} or {"$hybrid": ""}) first gets any rerankQuery / rerankOn error
    // (MISSING_RERANK_QUERY_TEXT, MISSING_RERANK_ON, or an invalid rerankOn path)
    if (legMode != LegMode.HYBRID) {
      throw RequestException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT.get();
    }
    if (legs.isEmpty()) {
      throw RequestException.Code.MISSING_HYBRID_SORT_VALUE.get();
    }

    int limit =
        getOrDefault(
            options,
            FindAndRerankCommand.Options::limit,
            operationsConfig.defaultFindAndRerankLimit());
    return new FindAndRerankPlan(legs, rerankServiceDef, rerankingQuery, passageLocator, limit);
  }

  /**
   * Check that the collection supports the sorts that have a value (vector / vectorize / lexical),
   * throw if it does not.
   */
  private static void checkSortSupported(
      boolean explicitLexical, FindAndRerankSort sort, CollectionSchemaObject schemaObject) {

    var vectorConfig = schemaObject.vectorConfig();
    if ((sort.vectorSort() != null || sort.vectorizeSort() != null)
        && !vectorConfig.vectorEnabled()) {
      throw SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION.get(errVars(schemaObject));
    }

    // the service definition is on the $vectorize field, not $vector
    if (sort.vectorizeSort() != null
        && vectorConfig
            .getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD)
            .map(VectorColumnDefinition::vectorizeDefinition)
            .isEmpty()) {
      throw SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION.get(errVars(schemaObject));
    }

    // The index is only required if the user explicitly asked for lexical,
    // they could also have used $hybrid and it expanded into the lexical sort.
    if (sort.lexicalSort() != null && explicitLexical && !schemaObject.lexicalDef().enabled()) {
      throw SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION.get(errVars(schemaObject));
    }
  }

  /**
   * Validate user-supplied hybridLimits against the dynamic OperationsConfig bounds. Both values
   * are checked, even when one of the legs will not run.
   */
  private static void checkHybridLimits(
      FindAndRerankCommand.Options options, OperationsConfig operationsConfig) {

    if (options == null || options.hybridLimits() == null) {
      return;
    }
    var hybridLimits = options.hybridLimits();
    checkLimitInBounds(
        "hybridLimits.$vector",
        hybridLimits.vectorLimit(),
        operationsConfig.hybridSearchVectorLimit());
    checkLimitInBounds(
        "hybridLimits.$lexical",
        hybridLimits.lexicalLimit(),
        operationsConfig.hybridSearchLexicalLimit());
  }

  private static void checkLimitInBounds(String field, int value, IntConfigWithBounds bounds) {
    if (bounds.isValid(value)) {
      return;
    }
    throw RequestException.Code.COMMAND_FIELD_VALUE_INVALID.get(
        Map.of(
            "field",
            field,
            "value",
            String.valueOf(value),
            "message",
            "must be between %d and %d (inclusive)".formatted(bounds.min(), bounds.max())));
  }

  /**
   * Resolve the reranking service for the command: a command override replaces the collection
   * config entirely, otherwise use the collection's configured service. Any non-null {@code rerank}
   * payload is treated as an override and validated, so {@code "rerank": {}} fails with {@link
   * RequestException.Code#INVALID_RERANK_OVERRIDE}. Throws {@link
   * RequestException.Code#UNSUPPORTED_RERANKING_COMMAND} when there is no override and the
   * collection has reranking disabled.
   */
  private static CollectionRerankDef.RerankServiceDef resolveRerankServiceDef(
      FindAndRerankCommand.Options options,
      CollectionSchemaObject schemaObject,
      RerankingProvidersConfig rerankingProvidersConfig) {

    var rerankOverride =
        getOrDefault(options, FindAndRerankCommand.Options::rerankServiceOverride, null);
    if (rerankOverride != null) {
      return CollectionRerankDef.validateServiceDesc(
          rerankingProvidersConfig,
          rerankOverride.provider(),
          rerankOverride.modelName(),
          rerankOverride.authentication(),
          rerankOverride.parameters(),
          RequestException.Code.INVALID_RERANK_OVERRIDE);
    }

    if (!schemaObject.rerankDef().enabled()) {
      throw RequestException.Code.UNSUPPORTED_RERANKING_COMMAND.get();
    }
    // Collection defaults: check END_OF_LIFE since model may have become EOL after creation.
    var serviceDef = schemaObject.rerankDef().rerankServiceDef();
    CollectionRerankDef.checkExistingModelStatus(rerankingProvidersConfig, serviceDef);
    return serviceDef;
  }

  /**
   * One leg for each <code>$hybrid</code> sort that has a value. The lexical leg comes first, so in
   * a two-leg request read task 0 is lexical and read task 1 is vector / vectorize.
   */
  private static List<RerankLeg> hybridLegs(
      FindAndRerankSort sort,
      FindAndRerankCommand.Options options,
      CollectionSchemaObject schemaObject) {

    var hybridLimits =
        getOrDefault(
            options,
            FindAndRerankCommand.Options::hybridLimits,
            FindAndRerankCommand.HybridLimits.DEFAULT);
    var legs = new ArrayList<RerankLeg>(2);

    // without a lexical index the lexical sort came from {"$hybrid": "text"} and is skipped,
    // an explicit $lexical has already failed in checkSortSupported()
    if (sort.lexicalSort() != null && schemaObject.lexicalDef().enabled()) {
      legs.add(new LexicalLeg(sort.lexicalSort(), hybridLimits.lexicalLimit()));
    }

    // $vectorize wins when the sort has both $vectorize and $vector
    if (sort.vectorizeSort() != null) {
      var vectorColumn =
          schemaObject
              .vectorConfig()
              .getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD)
              .orElseThrow();
      legs.add(
          new VectorizeLeg(
              sort.vectorizeSort(),
              vectorColumn.vectorSize(),
              vectorColumn.vectorizeDefinition(),
              hybridLimits.vectorLimit()));
    } else if (sort.vectorSort() != null) {
      legs.add(new VectorLeg(sort.vectorSort(), hybridLimits.vectorLimit()));
    }
    return legs;
  }

  /**
   * The field to rerank on: <code>options.rerankOn</code> when it is not blank, otherwise the
   * default of the first leg that has one, otherwise throws {@link
   * RequestException.Code#MISSING_RERANK_ON}.
   */
  private static String passageField(FindAndRerankCommand.Options options, List<RerankLeg> legs) {

    var rerankOn = getOrDefault(options, FindAndRerankCommand.Options::rerankOn, null);
    if (rerankOn != null && !rerankOn.isBlank()) {
      return rerankOn;
    }
    var defaultField = firstLegDefault(legs, RerankLeg::defaultPassageField);
    if (defaultField != null) {
      return defaultField;
    }
    throw RequestException.Code.MISSING_RERANK_ON.get();
  }

  /** The first default the legs have, or <code>null</code> if none of them has one. */
  private static String firstLegDefault(
      List<RerankLeg> legs, Function<RerankLeg, Optional<String>> getter) {
    return legs.stream().map(getter).flatMap(Optional::stream).findFirst().orElse(null);
  }
}
