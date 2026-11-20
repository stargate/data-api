package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.LEXICAL_CONTENT_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.LexicalSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorizeSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.config.IntConfigWithBounds;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.operation.reranking.RerankingQuery;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import io.stargate.sgv2.jsonapi.util.StringUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Validates a {@link FindAndRerankCommand} and translates the intention of the user, expressed in
 * the command, into a {@link FindAndRerankPlan} that is used to build the operation that runs it.
 */
class FindAndRerankPlanner {

  private FindAndRerankPlanner() {
    // static methods only
  }

  /**
   * Validates the command and makes its plan.
   *
   * @param command The command to plan.
   * @param commandContext Context for the command, it has the collection and the config.
   * @return The plan. A $hybrid sort always has a VECTOR_LEG, any other sort has one leg.
   */
  static FindAndRerankPlan plan(
      FindAndRerankCommand command, CommandContext<CollectionSchemaObject> commandContext) {

    Objects.requireNonNull(command, "command must not be null");
    Objects.requireNonNull(commandContext, "commandContext must not be null");

    var sort = command.sortClause();
    var schemaObject = commandContext.schemaObject();
    var operationsConfig = commandContext.config().get(OperationsConfig.class);

    validateSortSupported(sort, schemaObject);
    validateHybridLimits(command, operationsConfig);
    var rerankCandidateLimit = resolveRerankCandidateLimit(command, operationsConfig);

    var rerankServiceDef = resolveRerankServiceDef(command, commandContext);
    var rerankingQuery = buildRerankingQuery(command);
    var passageLocator = buildPassageLocator(command);
    var projector = command.buildProjector();

    // NOTE: the legs are planned after the options and the projection are checked, to keep the
    // error order of the command: e.g. {"$hybrid": ""} with no rerankQuery fails with
    // MISSING_RERANK_QUERY_TEXT, not MISSING_HYBRID_SORT_VALUE. And a command with no sort and no
    // options fails with MISSING_RERANK_QUERY_TEXT, not UNSUPPORTED_FIND_AND_RERANK_SORT.

    // NOTE: a sort with no $vector or $vectorize fails here with a SORT error, not later with an
    // UNEXPECTED_SERVER_ERROR, see GH #2577
    var legs =
        switch (sort) {
          case FindAndRerankSort.MultiLeg multiLeg ->
              buildMultiLegs(command, multiLeg, schemaObject);
          case FindAndRerankSort.SingleLeg singleLeg ->
              List.of(buildSingleLeg(singleLeg, rerankCandidateLimit));
        };
    var vectorizeStep = isVectorizeSort(sort) ? buildVectorizeStep(sort, schemaObject) : null;

    int limit =
        getOrDefault(
            command.options(),
            FindAndRerankCommand.Options::limit,
            operationsConfig.defaultFindAndRerankLimit());

    return new FindAndRerankPlan(
        vectorizeStep,
        legs,
        new FindAndRerankPlan.RerankStep(
            rerankServiceDef, rerankingQuery, passageLocator, projector, limit),
        getOrDefault(command.options(), FindAndRerankCommand.Options::includeScores, false),
        getOrDefault(command.options(), FindAndRerankCommand.Options::includeSortVector, false));
  }

  /**
   * Validate user-supplied hybridLimits: they can only be used with a $hybrid sort, and must be
   * inside the dynamic OperationsConfig bounds. Cannot be done in the Jackson deserializer because
   * it has no access to the sort or to OperationsConfig (config-file and env-var overrides).
   */
  private static void validateHybridLimits(
      FindAndRerankCommand command, OperationsConfig operationsConfig) {
    if (command.options() == null || command.options().hybridLimits() == null) {
      return;
    }
    if (command.sortClause() instanceof FindAndRerankSort.SingleLeg) {
      throw RequestException.Code.UNSUPPORTED_HYBRID_LIMITS_WITHOUT_HYBRID_SORT.get();
    }
    var hybridLimits = command.options().hybridLimits();
    validateLimitInBounds(
        FindAndRerankCommand.HybridLimitsDeserializer.VECTOR_LIMIT_FIELD,
        hybridLimits.vectorLimit(),
        operationsConfig.hybridSearchVectorLimit());
    validateLimitInBounds(
        FindAndRerankCommand.HybridLimitsDeserializer.LEXICAL_LIMIT_FIELD,
        hybridLimits.lexicalLimit(),
        operationsConfig.hybridSearchLexicalLimit());
  }

  /**
   * Resolves the read limit for a sort without $hybrid.
   *
   * @param command The command. It may have the <code>rerankCandidateLimit</code> option.
   * @param operationsConfig The config that has the default and the bounds for the read limit.
   * @return The <code>rerankCandidateLimit</code> option, or the default from the config when the
   *     option is not set.
   */
  private static int resolveRerankCandidateLimit(
      FindAndRerankCommand command, OperationsConfig operationsConfig) {
    var bounds = operationsConfig.rerankCandidateLimit();
    var limit =
        getOrDefault(command.options(), FindAndRerankCommand.Options::rerankCandidateLimit, null);
    if (limit == null) {
      // not set on the command, so use the default from the config
      return bounds.defaultValue();
    }
    // a $hybrid sort sets the read limit of each leg with the hybridLimits option instead
    if (command.sortClause() instanceof FindAndRerankSort.MultiLeg) {
      throw RequestException.Code.UNSUPPORTED_RERANK_CANDIDATE_LIMIT_WITH_HYBRID_SORT.get();
    }
    validateLimitInBounds(FindAndRerankCommand.Options.Names.RERANK_CANDIDATE_LIMIT, limit, bounds);
    return limit;
  }

  private static void validateLimitInBounds(String field, int value, IntConfigWithBounds bounds) {
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
   * Validate that the collection supports the sort features the request uses (vector / vectorize /
   * lexical), throw if it does not.
   */
  private static void validateSortSupported(
      FindAndRerankSort sort, CollectionSchemaObject schemaObject) {

    if (isVectorSort(sort) || isVectorizeSort(sort)) {
      if (!schemaObject.vectorConfig().vectorEnabled()) {
        throw SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION.get(errVars(schemaObject));
      }
    }

    if (isVectorizeSort(sort)) {
      // the service definition is on the $vectorize field, not $vector
      var vectorizeEnabled =
          schemaObject
              .vectorConfig()
              .getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD)
              .map(VectorColumnDefinition::vectorizeDefinition)
              .isPresent();

      if (!vectorizeEnabled) {
        throw SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION.get(
            errVars(schemaObject));
      }
    }

    // The index is only required if the user explicitly asked for lexical,
    // the $hybrid text skips the lexical leg when there is no index.
    if (isExplicitLexicalSort(sort)) {
      throwIfNoLexicalIndex(schemaObject);
    }
  }

  /**
   * Builds the legs for a $hybrid sort.
   *
   * @param command The command. Its <code>hybridLimits</code> option sets the read limit of each
   *     leg.
   * @param multiLeg The $hybrid sort of the command.
   * @param schemaObject The collection. The lexical leg needs its lexical index.
   * @return A vector leg, and a lexical leg when the sort has lexical text and the collection has a
   *     lexical index.
   */
  private static List<FindAndRerankPlan.RerankLeg> buildMultiLegs(
      FindAndRerankCommand command,
      FindAndRerankSort.MultiLeg multiLeg,
      CollectionSchemaObject schemaObject) {

    // every $hybrid sort reads with the vector leg, so it needs $vectorize text or a $vector
    if (!isVectorizeSort(multiLeg) && !isVectorSort(multiLeg)) {
      throw isLexicalSort(multiLeg)
          ? SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT.get()
          : SortException.Code.MISSING_HYBRID_SORT_VALUE.get();
    }

    var hybridLimits =
        getOrDefault(
            command.options(),
            FindAndRerankCommand.Options::hybridLimits,
            FindAndRerankCommand.HybridLimits.DEFAULT);
    var legs = new ArrayList<FindAndRerankPlan.RerankLeg>(2);

    // the vector leg reads with the vector from the vectorize step when there is text to vectorize,
    // otherwise with the vector from the command
    // NOTE: when the sort has both, $vectorize wins and $vector is ignored
    // TODO: GH #2616 - reject a sort that has both $vectorize and $vector
    legs.add(
        new FindAndRerankPlan.RerankLeg(
            RerankLegType.VECTOR_LEG,
            isVectorizeSort(multiLeg) ? null : vector(multiLeg),
            null,
            hybridLimits.vectorLimit()));

    // the lexical leg is optional: if there is a lexical sort, but the user did not ask explicitly
    // for it then it is OK to skip when there is no lexical index.
    if (isLexicalSort(multiLeg)) {
      if (schemaObject.lexicalDef().enabled()) {
        legs.add(
            new FindAndRerankPlan.RerankLeg(
                RerankLegType.LEXICAL_LEG,
                null,
                lexicalText(multiLeg),
                hybridLimits.lexicalLimit()));
      } else if (isExplicitLexicalSort(multiLeg)) {
        // error should have been caught in validateSortSupported() safety here
        throwIfNoLexicalIndex(schemaObject);
      }
    }
    return legs;
  }

  /**
   * Builds the leg for a sort without $hybrid.
   *
   * @param singleLeg The sort of the command.
   * @param readLimit The maximum number of documents the leg reads.
   * @return A vector leg for a $vectorize or $vector sort, or a lexical leg for a $lexical sort.
   */
  private static FindAndRerankPlan.RerankLeg buildSingleLeg(
      FindAndRerankSort.SingleLeg singleLeg, int readLimit) {
    return switch (singleLeg) {
        // the vector leg reads with the vector from the vectorize step
      case FindAndRerankSort.VectorLeg(VectorizeSortExpression ignored) ->
          new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, null, null, readLimit);
      case FindAndRerankSort.VectorLeg(VectorSortExpression(var vector)) ->
          new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, vector, null, readLimit);
      case FindAndRerankSort.LexicalLeg(LexicalSortExpression(var lexicalText)) ->
          new FindAndRerankPlan.RerankLeg(RerankLegType.LEXICAL_LEG, null, lexicalText, readLimit);
        // TODO: GH #2603 - plan the filter leg. Until then a field sort, an empty sort and no sort
        // are rejected here, after the checks in plan()
      case FindAndRerankSort.FilterLeg ignored ->
          throw SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT.get();
    };
  }

  /**
   * Builds the {@link FindAndRerankPlan.VectorizeStep} for the vectorize text of the sort.
   *
   * @param sort The sort of the command. It must have vectorize text.
   * @param schemaObject The collection. The step uses its vectorize column.
   * @return The step that vectorizes the text with the vectorize service of the collection.
   */
  private static FindAndRerankPlan.VectorizeStep buildVectorizeStep(
      FindAndRerankSort sort, CollectionSchemaObject schemaObject) {

    // sanity check, validateSortSupported() checked the collection has a vectorize column
    var vectorizeColumn =
        schemaObject.vectorConfig().getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD).orElseThrow();
    return new FindAndRerankPlan.VectorizeStep(
        vectorizeText(sort), vectorizeColumn.vectorSize(), vectorizeColumn.vectorizeDefinition());
  }

  /**
   * Resolve the effective {@link CollectionRerankDef.RerankServiceDef} for this command: a command
   * override replaces the collection-level config entirely; otherwise fall back to the collection's
   * configured defaults. Any non-null {@code rerank} payload is treated as an override attempt and
   * validated — {@code "rerank": {}} or a payload whose fields are all explicit nulls fails with
   * {@link RequestException.Code#INVALID_RERANK_OVERRIDE} rather than silently falling back. Throws
   * {@link RequestException.Code#UNSUPPORTED_RERANKING_COMMAND} when no override is supplied and
   * the collection itself has reranking disabled.
   */
  private static CollectionRerankDef.RerankServiceDef resolveRerankServiceDef(
      FindAndRerankCommand command, CommandContext<CollectionSchemaObject> commandContext) {
    var rerankOverride =
        getOrDefault(command.options(), FindAndRerankCommand.Options::rerankServiceOverride, null);
    boolean hasOverride = rerankOverride != null;

    if (!commandContext.schemaObject().rerankDef().enabled() && !hasOverride) {
      throw RequestException.Code.UNSUPPORTED_RERANKING_COMMAND.get();
    }

    var rerankingProvidersConfig = commandContext.rerankingProviderFactory().getRerankingConfig();

    if (hasOverride) {
      return CollectionRerankDef.validateServiceDesc(
          rerankingProvidersConfig,
          rerankOverride.provider(),
          rerankOverride.modelName(),
          rerankOverride.authentication(),
          rerankOverride.parameters(),
          RequestException.Code.INVALID_RERANK_OVERRIDE);
    }
    // Collection defaults: check END_OF_LIFE since model may have become EOL after creation.
    var serviceDef = commandContext.schemaObject().rerankDef().rerankServiceDef();
    CollectionRerankDef.checkExistingModelStatus(rerankingProvidersConfig, serviceDef);
    return serviceDef;
  }

  /**
   * Builds the query to rerank with.
   *
   * @param command The command. It has the sort and may have the <code>rerankQuery</code> option.
   * @return The query from the <code>rerankQuery</code> option, or from the sort when the option is
   *     null or blank.
   */
  private static RerankingQuery buildRerankingQuery(FindAndRerankCommand command) {
    var sort = command.sortClause();
    var rerankQueryOption =
        getOrDefault(command.options(), FindAndRerankCommand.Options::rerankQuery, null);

    // the default query is the text of the sort: the lexical text for a $lexical sort, otherwise
    // the vectorize text for $vectorize or $hybrid
    return sort instanceof FindAndRerankSort.LexicalLeg(LexicalSortExpression(var lexicalText))
        ? RerankingQuery.forLexicalSort(rerankQueryOption, lexicalText)
        : RerankingQuery.forVectorizeSort(rerankQueryOption, vectorizeText(sort));
  }

  /**
   * Builds the locator that finds the passage to rerank in each document.
   *
   * @param command The command. It has the sort and may have the <code>rerankOn</code> option.
   * @return The locator for the field in the <code>rerankOn</code> option, or for the default field
   *     of the sort when the option is null or blank.
   */
  private static PathMatchLocator buildPassageLocator(FindAndRerankCommand command) {
    var sort = command.sortClause();
    var rerankOnOption =
        getOrDefault(command.options(), FindAndRerankCommand.Options::rerankOn, null);

    // the user set rerankOn, so it replaces the default field of the sort
    if (!StringUtil.isNullOrBlank(rerankOnOption)) {
      return PathMatchLocator.forPath(rerankOnOption);
    }
    // a $lexical sort reranks on the $lexical field by default
    if (sort instanceof FindAndRerankSort.LexicalLeg) {
      return PathMatchLocator.forPath(LEXICAL_CONTENT_FIELD);
    }
    // a sort with vectorize text, e.g. $vectorize or $hybrid, reranks on the $vectorize field by
    // default
    if (isVectorizeSort(sort)) {
      return PathMatchLocator.forPath(VECTOR_EMBEDDING_TEXT_FIELD);
    }
    // other sorts, e.g. $vector or a field sort, have no default field, so rerankOn is required
    throw RequestException.Code.MISSING_RERANK_ON.get();
  }

  private static void throwIfNoLexicalIndex(CollectionSchemaObject schemaObject) {
    if (!schemaObject.lexicalDef().enabled()) {
      throw SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION.get(errVars(schemaObject));
    }
  }

  // NOTE: the helpers below return null or false for a SingleLeg sort, because plan() rejects
  // single leg sorts until GH #2602 and GH #2603

  /** The text to vectorize: the $hybrid text, or the $vectorize value, or <code>null</code>. */
  private static String vectorizeText(FindAndRerankSort sort) {
    return switch (sort) {
      case FindAndRerankSort.HybridText hybridText -> hybridText.text();
      case FindAndRerankSort.HybridObject hybridObject -> hybridObject.vectorize();
      case FindAndRerankSort.VectorLeg(VectorizeSortExpression(var text)) -> text;
      case FindAndRerankSort.VectorLeg(VectorSortExpression ignored) -> null;
      case FindAndRerankSort.LexicalLeg ignored -> null;
      case FindAndRerankSort.FilterLeg ignored -> null;
    };
  }

  /**
   * The text for the lexical sort: the $hybrid text, or the $lexical value, or <code>null</code>.
   */
  private static String lexicalText(FindAndRerankSort sort) {
    return switch (sort) {
      case FindAndRerankSort.HybridText hybridText -> hybridText.text();
      case FindAndRerankSort.HybridObject hybridObject -> hybridObject.lexical();
      case FindAndRerankSort.LexicalLeg(LexicalSortExpression(var text)) -> text;
      case FindAndRerankSort.VectorLeg ignored -> null;
      case FindAndRerankSort.FilterLeg ignored -> null;
    };
  }

  /** The vector from the $hybrid object or from the $vector sort, or <code>null</code>. */
  private static float[] vector(FindAndRerankSort sort) {
    return switch (sort) {
      case FindAndRerankSort.HybridText ignored -> null;
      case FindAndRerankSort.HybridObject hybridObject -> hybridObject.vector();
      case FindAndRerankSort.VectorLeg(VectorSortExpression(var vector)) -> vector;
      case FindAndRerankSort.VectorLeg(VectorizeSortExpression ignored) -> null;
      case FindAndRerankSort.LexicalLeg ignored -> null;
      case FindAndRerankSort.FilterLeg ignored -> null;
    };
  }

  private static boolean isLexicalSort(FindAndRerankSort sort) {
    return lexicalText(sort) != null;
  }

  /**
   * The $hybrid object has $lexical text, or the sort is $lexical, so the collection must have a
   * lexical index.
   */
  private static boolean isExplicitLexicalSort(FindAndRerankSort sort) {
    return switch (sort) {
      case FindAndRerankSort.HybridText ignored -> false;
      case FindAndRerankSort.HybridObject hybridObject -> hybridObject.lexical() != null;
      case FindAndRerankSort.LexicalLeg ignored -> true;
      case FindAndRerankSort.VectorLeg ignored -> false;
      case FindAndRerankSort.FilterLeg ignored -> false;
    };
  }

  private static boolean isVectorizeSort(FindAndRerankSort sort) {
    return vectorizeText(sort) != null;
  }

  private static boolean isVectorSort(FindAndRerankSort sort) {
    return vector(sort) != null;
  }
}
