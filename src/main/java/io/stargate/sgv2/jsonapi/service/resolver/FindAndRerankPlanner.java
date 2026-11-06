package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.builders.CollectionSortClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.*;
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
   * @return The plan, it always has a VECTOR_LEG.
   */
  static FindAndRerankPlan plan(
      FindAndRerankCommand command, CommandContext<CollectionSchemaObject> commandContext) {

    Objects.requireNonNull(command, "command must not be null");
    Objects.requireNonNull(commandContext, "commandContext must not be null");

    var schemaObject = commandContext.schemaObject();
    var sort =
        command.sortClause() instanceof UnresolvedSingleLeg unresolved
            ? resolveSingleLeg(unresolved, schemaObject)
            : command.sortClause();
    var operationsConfig = commandContext.config().get(OperationsConfig.class);

    checkSortSupported(sort, schemaObject);
    validateHybridLimits(command, operationsConfig);
    var rerankServiceDef = resolveRerankServiceDef(command, commandContext);
    var rerankingQuery =
        RerankingQuery.create(
            getOrDefault(command.options(), FindAndRerankCommand.Options::rerankQuery, null),
            vectorizeText(sort));
    var passageLocator = passageLocator(command, sort);
    var projector = command.buildProjector();

    // NOTE: the legs are planned after the options and the projection are checked, to keep the
    // error order of the command: e.g. {"$hybrid": ""} with no rerankQuery fails with
    // MISSING_RERANK_QUERY_TEXT, not MISSING_HYBRID_SORT_VALUE

    // NOTE: a sort with no $vector or $vectorize fails here with a SORT error, not later with an
    // UNEXPECTED_SERVER_ERROR, see GH #2577
    var legs =
        switch (sort) {
          case MultiLeg multiLeg -> multiLegs(command, multiLeg, schemaObject);
            // TODO: GH #2602, GH #2603 - execute the validated single-leg sorts.
          case LexicalLeg ignored ->
              throw SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT.get();
          case VectorLeg ignored -> throw SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT.get();
          case FilterLeg ignored -> throw SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT.get();
          case UnresolvedSingleLeg ignored ->
              throw new IllegalStateException("sort was not resolved");
        };
    var vectorizeStep = isVectorizeSort(sort) ? planVectorizeStep(sort, schemaObject) : null;

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

  /** Validate single-leg values with find's builder before assigning their typed form. */
  static SingleLeg resolveSingleLeg(UnresolvedSingleLeg sort, CollectionSchemaObject schemaObject) {
    var clause = new CollectionSortClauseBuilder(schemaObject).build(sort.sort());
    clause.validate(schemaObject);
    var features = sort.commandFeatures();
    var lexical = clause.lexicalSortExpression();
    if (lexical != null) {
      return new LexicalLeg(List.of(new LexicalSe(lexical.getLexicalQuery())), features);
    }
    if (clause.hasVsearchClause()) {
      return new VectorLeg(
          List.of(new VectorSe(clause.sortExpressions().getFirst().getVector())), features);
    }
    if (clause.hasVectorizeSearchClause()) {
      return new VectorLeg(
          List.of(new VectorizeSe(clause.sortExpressions().getFirst().getVectorize())), features);
    }
    return new FilterLeg(
        clause.sortExpressions().stream()
            .<SortExpression2>map(
                expression -> new FieldSe(expression.getPath(), expression.isAscending()))
            .toList(),
        features);
  }

  /**
   * Validate user-supplied hybridLimits against the dynamic OperationsConfig bounds. Cannot be done
   * in the Jackson deserializer because it has no access to OperationsConfig (config-file and
   * env-var overrides).
   */
  private static void validateHybridLimits(
      FindAndRerankCommand command, OperationsConfig operationsConfig) {
    if (command.options() == null || command.options().hybridLimits() == null) {
      return;
    }
    var hybridLimits = command.options().hybridLimits();
    checkLimitInBounds(
        FindAndRerankCommand.HybridLimitsDeserializer.VECTOR_LIMIT_FIELD,
        hybridLimits.vectorLimit(),
        operationsConfig.hybridSearchVectorLimit());
    checkLimitInBounds(
        FindAndRerankCommand.HybridLimitsDeserializer.LEXICAL_LIMIT_FIELD,
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
   * Check that collection supports the sort features the request uses (vector / vectorize /
   * lexical), throw if it does not.
   */
  private static void checkSortSupported(
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
   * The legs of a $hybrid sort: always a vector leg, and a lexical leg when the sort has lexical
   * text and the collection has a lexical index. Without a lexical index the $hybrid text skips the
   * lexical leg, and an explicit $lexical throws. Throws when the sort has no vectorize text and no
   * vector for the vector leg.
   */
  private static List<FindAndRerankPlan.RerankLeg> multiLegs(
      FindAndRerankCommand command, MultiLeg sort, CollectionSchemaObject schemaObject) {

    if (!isVectorizeSort(sort) && !isVectorSort(sort)) {
      throw isLexicalSort(sort)
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
            isVectorizeSort(sort) ? null : vector(sort),
            null,
            hybridLimits.vectorLimit()));

    // the lexical leg is optional: if there is a lexical sort, but the user did not ask explicitly
    // for it then it is OK to skip when there is no lexical index.
    if (isLexicalSort(sort)) {
      if (schemaObject.lexicalDef().enabled()) {
        legs.add(
            new FindAndRerankPlan.RerankLeg(
                RerankLegType.LEXICAL_LEG, null, lexicalText(sort), hybridLimits.lexicalLimit()));
      } else if (isExplicitLexicalSort(sort)) {
        // error should have been caught in checkSortSupported() safety here
        throwIfNoLexicalIndex(schemaObject);
      }
    }
    return legs;
  }

  /**
   * The {@link FindAndRerankPlan.VectorizeStep} for the vectorize text of the sort, using the
   * vectorize column of the collection.
   */
  private static FindAndRerankPlan.VectorizeStep planVectorizeStep(
      FindAndRerankSort sort, CollectionSchemaObject schemaObject) {

    // sanity check, checkSortSupported() checked the collection has a vectorize column
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

  private static PathMatchLocator passageLocator(
      FindAndRerankCommand command, FindAndRerankSort sort) {

    var rerankOn = getOrDefault(command.options(), FindAndRerankCommand.Options::rerankOn, null);
    var isRerankOn = rerankOn != null && !rerankOn.isBlank();

    String finalRerankField;

    if (isVectorizeSort(sort)) {
      // use the vectorize field, unless the user has overridden
      finalRerankField = isRerankOn ? rerankOn : VECTOR_EMBEDDING_TEXT_FIELD;
    } else if (isRerankOn) {
      // user has to provide a field to rerank on
      finalRerankField = rerankOn;
    } else {
      throw RequestException.Code.MISSING_RERANK_ON.get();
    }

    return PathMatchLocator.forPath(finalRerankField);
  }

  private static void throwIfNoLexicalIndex(CollectionSchemaObject schemaObject) {
    if (!schemaObject.lexicalDef().enabled()) {
      throw SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION.get(errVars(schemaObject));
    }
  }

  /** The text to vectorize: the $hybrid text, or the $vectorize value, or <code>null</code>. */
  private static String vectorizeText(FindAndRerankSort sort) {
    return switch (sort) {
      case HybridText text -> text.text();
      case HybridObject object -> object.vectorize();
      case VectorLeg leg ->
          switch (leg.sortExpressions().getFirst()) {
            case VectorizeSe expression -> expression.vectorizeText();
            case VectorSe ignored -> null;
            case LexicalSe ignored ->
                throw new IllegalStateException("lexical expression in vector leg");
            case FieldSe ignored ->
                throw new IllegalStateException("field expression in vector leg");
          };
      case LexicalLeg ignored -> null;
      case FilterLeg ignored -> null;
      case UnresolvedSingleLeg ignored -> throw new IllegalStateException("sort was not resolved");
    };
  }

  /**
   * The text for the lexical sort: the $hybrid text, or the $lexical value, or <code>null</code>.
   */
  private static String lexicalText(FindAndRerankSort sort) {
    return switch (sort) {
      case HybridText text -> text.text();
      case HybridObject object -> object.lexical();
      case LexicalLeg leg -> ((LexicalSe) leg.sortExpressions().getFirst()).lexicalText();
      case VectorLeg ignored -> null;
      case FilterLeg ignored -> null;
      case UnresolvedSingleLeg ignored -> throw new IllegalStateException("sort was not resolved");
    };
  }

  private static boolean isLexicalSort(FindAndRerankSort sort) {
    return lexicalText(sort) != null;
  }

  /** The $hybrid object has $lexical text, so the collection must have a lexical index. */
  private static boolean isExplicitLexicalSort(FindAndRerankSort sort) {
    return switch (sort) {
      case HybridObject object -> object.lexical() != null;
      case LexicalLeg ignored -> true;
      case HybridText ignored -> false;
      case VectorLeg ignored -> false;
      case FilterLeg ignored -> false;
      case UnresolvedSingleLeg ignored -> throw new IllegalStateException("sort was not resolved");
    };
  }

  private static boolean isVectorizeSort(FindAndRerankSort sort) {
    return vectorizeText(sort) != null;
  }

  private static boolean isVectorSort(FindAndRerankSort sort) {
    return vector(sort) != null;
  }

  private static float[] vector(FindAndRerankSort sort) {
    return switch (sort) {
      case HybridObject object -> object.vector();
      case VectorLeg leg ->
          switch (leg.sortExpressions().getFirst()) {
            case VectorSe expression -> expression.vector();
            case VectorizeSe ignored -> null;
            case LexicalSe ignored ->
                throw new IllegalStateException("lexical expression in vector leg");
            case FieldSe ignored ->
                throw new IllegalStateException("field expression in vector leg");
          };
      case HybridText ignored -> null;
      case LexicalLeg ignored -> null;
      case FilterLeg ignored -> null;
      case UnresolvedSingleLeg ignored -> throw new IllegalStateException("sort was not resolved");
    };
  }
}
