package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.LEXICAL_CONTENT_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD;
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
 * <p>The planner validates the request and throws if it cannot be run.
 *
 * <p>The {@link LegMode} is what the user asked for, the legs are what will run. They can be
 * different, for example <code>{"$hybrid": "cheese"}</code> skips the lexical leg when the
 * collection has no lexical index.
 */
public class FindAndRerankPlanner {

  // the field names in the hybridLimits errors, the JSON names HybridLimitsDeserializer reads
  private static final String HYBRID_LIMITS_VECTOR_FIELD = "hybridLimits." + VECTOR_EMBEDDING_FIELD;
  private static final String HYBRID_LIMITS_LEXICAL_FIELD = "hybridLimits." + LEXICAL_CONTENT_FIELD;

  private FindAndRerankPlanner() {
    // prevent instantiation
  }

  /**
   * Validates the command and decides the legs, the reranking service, the rerank query and
   * passage, and the number of documents to return.
   *
   * @param sort The parsed sort, its {@link LegMode} decides which legs can run.
   * @param options The command options, may be <code>null</code>.
   * @param schemaObject The collection the command runs against.
   * @param operationsConfig Config for the bounds of <code>hybridLimits</code> and the default
   *     limit.
   * @param rerankingProvidersConfig Config used to validate the reranking service.
   * @return The plan, it always has at least one leg.
   */
  public static FindAndRerankPlan plan(
      FindAndRerankSort sort,
      FindAndRerankCommand.Options options,
      CollectionSchemaObject schemaObject,
      OperationsConfig operationsConfig,
      RerankingProvidersConfig rerankingProvidersConfig) {

    Objects.requireNonNull(sort, "sort must not be null");
    Objects.requireNonNull(schemaObject, "schemaObject must not be null");
    Objects.requireNonNull(operationsConfig, "operationsConfig must not be null");
    Objects.requireNonNull(rerankingProvidersConfig, "rerankingProvidersConfig must not be null");

    checkSortSupported(sort, schemaObject);
    checkHybridLimits(options, operationsConfig);
    var rerankServiceDef = resolveRerankServiceDef(options, schemaObject, rerankingProvidersConfig);

    // TODO: GH #2602, GH #2603 - the single leg modes and FILTER have no legs yet and fail below
    // with UNSUPPORTED_FIND_AND_RERANK_SORT, until the planner can build their legs
    List<RerankLeg> legs =
        switch (sort.legMode()) {
          case HYBRID -> hybridLegs(sort, options, schemaObject);
          case VECTORIZE, VECTOR, LEXICAL, FILTER -> List.of();
        };

    // only the vectorize leg has a default query, so the leg default is the vectorize query
    var rerankingQuery =
        RerankingQuery.create(
            getOrDefault(options, FindAndRerankCommand.Options::rerankQuery, null),
            firstLegDefault(legs, RerankLeg::defaultRerankQuery).orElse(null));
    var passageLocator = PathMatchLocator.forPath(passageField(options, legs));

    // these come after the rerank query and passage checks, so a request with no valued sort (e.g.
    // {} or {"$hybrid": ""}) first gets any rerankQuery / rerankOn error
    // (MISSING_RERANK_QUERY_TEXT, MISSING_RERANK_ON, or an invalid rerankOn path)
    // TODO: Update it when support LegMode.FILTER
    if (sort.legMode() != LegMode.HYBRID) {
      throw SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT.get();
    }
    if (legs.isEmpty()) {
      throw SortException.Code.MISSING_HYBRID_SORT_VALUE.get();
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
   * throw if it does not. The text of <code>{"$hybrid": "text"}</code> needs vectorize, but not a
   * lexical index.
   *
   * <p>Also check that a <code>$hybrid</code> object with a <code>$lexical</code> value has a
   * <code>$vectorize</code> or <code>$vector</code> value, a <code>$hybrid</code> sort always has a
   * vector or vectorize leg.
   */
  private static void checkSortSupported(
      FindAndRerankSort sort, CollectionSchemaObject schemaObject) {

    // the $hybrid text is always read with vectorize
    var needsVectorize = sort.hybridText() != null || sort.vectorizeSort() != null;

    var vectorConfig = schemaObject.vectorConfig();
    if ((needsVectorize || sort.vectorSort() != null) && !vectorConfig.vectorEnabled()) {
      throw SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION.get(errVars(schemaObject));
    }

    // the service definition is on the $vectorize field, not $vector
    var vectorizeEnabled =
        vectorConfig
            .getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD)
            .map(VectorColumnDefinition::vectorizeDefinition)
            .isPresent();
    if (needsVectorize && !vectorizeEnabled) {
      throw SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION.get(errVars(schemaObject));
    }

    // only a $lexical value needs the index, the lexical leg for the $hybrid text is skipped
    // without one
    if (sort.lexicalSort() != null) {
      throwIfNoLexicalIndex(schemaObject);
    }

    // after the lexical index check, so a missing index is reported first. A sort with no value
    // at all fails later in plan() with MISSING_HYBRID_SORT_VALUE.
    // NOTE: reading with only $lexical will be the single leg sort {"$lexical": "text"}, GH #2602
    if (sort.legMode() == LegMode.HYBRID
        && sort.lexicalSort() != null
        && sort.vectorizeSort() == null
        && sort.vectorSort() == null) {
      throw SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT.get();
    }
  }

  private static void throwIfNoLexicalIndex(CollectionSchemaObject schemaObject) {
    if (!schemaObject.lexicalDef().enabled()) {
      throw SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION.get(errVars(schemaObject));
    }
  }

  /**
   * Validate user-supplied hybridLimits against the dynamic OperationsConfig bounds. Cannot be done
   * in the Jackson deserializer because it has no access to OperationsConfig (config-file and
   * env-var overrides). Both values are checked, even when one of the legs will not run.
   */
  private static void checkHybridLimits(
      FindAndRerankCommand.Options options, OperationsConfig operationsConfig) {

    var hybridLimits = getOrDefault(options, FindAndRerankCommand.Options::hybridLimits, null);
    if (hybridLimits == null) {
      return;
    }
    checkLimitInBounds(
        HYBRID_LIMITS_VECTOR_FIELD,
        hybridLimits.vectorLimit(),
        operationsConfig.hybridSearchVectorLimit());
    checkLimitInBounds(
        HYBRID_LIMITS_LEXICAL_FIELD,
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
   * The legs for a <code>$hybrid</code> sort: at most one lexical leg, then one vectorize or vector
   * leg. So in a two-leg request read task 0 is lexical and read task 1 is vector / vectorize.
   *
   * <p>The text of <code>{"$hybrid": "text"}</code> is read by a vectorize leg, and by a lexical
   * leg when the collection has a lexical index. The <code>$hybrid</code> object runs a lexical leg
   * when there is <code>$lexical</code> text, then a vectorize leg, or a vector leg when there is
   * no <code>$vectorize</code> text.
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

    if (sort.hybridText() != null) {
      if (schemaObject.lexicalDef().enabled()) {
        legs.add(new LexicalLeg(sort.hybridText(), hybridLimits.lexicalLimit()));
      }
      legs.add(vectorizeLeg(sort.hybridText(), schemaObject, hybridLimits));
      return legs;
    }

    if (sort.lexicalSort() != null) {
      // should have been caught in checkSortSupported(), sanity check
      throwIfNoLexicalIndex(schemaObject);
      legs.add(new LexicalLeg(sort.lexicalSort(), hybridLimits.lexicalLimit()));
    }

    // $vectorize wins when the sort has both $vectorize and $vector, as it did before the planner
    // TODO: GH #2616 - reject a sort that has both $vectorize and $vector
    if (sort.vectorizeSort() != null) {
      legs.add(vectorizeLeg(sort.vectorizeSort(), schemaObject, hybridLimits));
    } else if (sort.vectorSort() != null) {
      legs.add(new VectorLeg(sort.vectorSort(), hybridLimits.vectorLimit()));
    }
    return legs;
  }

  /** A vectorize leg for the text, using the vector column and service of the collection. */
  private static VectorizeLeg vectorizeLeg(
      String vectorizeText,
      CollectionSchemaObject schemaObject,
      FindAndRerankCommand.HybridLimits hybridLimits) {

    // cannot be empty, checkSortSupported() has already thrown
    // UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION when there is no $vectorize column. If the steps
    // in plan() are reordered this orElseThrow() fails with UNEXPECTED_SERVER_ERROR.
    var vectorColumn =
        schemaObject.vectorConfig().getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD).orElseThrow();
    return new VectorizeLeg(
        vectorizeText,
        vectorColumn.vectorSize(),
        vectorColumn.vectorizeDefinition(),
        hybridLimits.vectorLimit());
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
    return firstLegDefault(legs, RerankLeg::defaultPassageField)
        .orElseThrow(() -> RequestException.Code.MISSING_RERANK_ON.get());
  }

  /** The first default the legs have, empty if none of them has one. */
  private static Optional<String> firstLegDefault(
      List<RerankLeg> legs, Function<RerankLeg, Optional<String>> getter) {
    return legs.stream().map(getter).flatMap(Optional::stream).findFirst();
  }
}
