package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;

/**
 * Resolves the {@link CreateCollectionCommand } command into a {@link CreateCollectionOperation}
 */
@ApplicationScoped
public class CreateCollectionCommandResolver implements CommandResolver<CreateCollectionCommand> {

  private final ObjectMapper objectMapper;
  private final DocumentLimitsConfig documentLimitsConfig;
  private final DatabaseLimitsConfig dbLimitsConfig;
  private final OperationsConfig operationsConfig;
  private final VectorizeConfigValidator validateVectorize;
  private final RerankingProvidersConfig rerankingProvidersConfig;

  @Inject
  public CreateCollectionCommandResolver(
      DocumentLimitsConfig documentLimitsConfig,
      DatabaseLimitsConfig dbLimitsConfig,
      OperationsConfig operationsConfig,
      VectorizeConfigValidator validateVectorize,
      RerankingProvidersConfig rerankingProvidersConfig,
      ObjectMapper objectMapper) {
    this.documentLimitsConfig = documentLimitsConfig;
    this.dbLimitsConfig = dbLimitsConfig;
    this.operationsConfig = operationsConfig;
    this.validateVectorize = validateVectorize;
    this.rerankingProvidersConfig = rerankingProvidersConfig;
    this.objectMapper = objectMapper;
  }

  @Override
  public Class<CreateCollectionCommand> getCommandClass() {
    return CreateCollectionCommand.class;
  }

  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> context, CreateCollectionCommand command) {

    var collectionName =
        cqlIdentifierFromUserInput(NamingRules.COLLECTION.checkRule(command.name()));

    // for these config options we only have the public API sided *Desc classes
    // no different internal representation
    var docIdDesc =
        getOrDefault(command.options(), CreateCollectionCommand.Options::idConfig, null);

    var vectorSearchDesc =
        getOrDefault(command.options(), CreateCollectionCommand.Options::vector, null);
    if (vectorSearchDesc != null) {
      vectorSearchDesc = validateVectorOptions(vectorSearchDesc);
    }

    var indexingDesc =
        getOrDefault(command.options(), CreateCollectionCommand.Options::indexing, null);
    if (indexingDesc != null) {
      indexingDesc.validate();
    }

    // for these config options we have a *Def internal representation that we build from the
    // public API sided *Desc classes
    var lexicalDef =
        CollectionLexicalDef.fromApiDesc(
            objectMapper,
            getOrDefault(command.options(), CreateCollectionCommand.Options::lexical, null),
            context.versionedSchema().lexicalDef());

    var rerankDef =
        CollectionRerankDef.fromApiDesc(
            getOrDefault(command.options(), CreateCollectionCommand.Options::rerank, null),
            rerankingProvidersConfig,
            context.versionedSchema().rerankDef());

    return new CreateCollectionOperation(
        context,
        dbLimitsConfig,
        collectionName,
        operationsConfig.databaseConfig().ddlDelayMillis(),
        operationsConfig.tooManyIndexesRollbackEnabled(),
        docIdDesc,
        indexingDesc,
        vectorSearchDesc,
        lexicalDef,
        rerankDef);
  }

  /**
   * Validates the vector search options provided in a create collection command. It checks if
   * vector search is enabled globally, and validates the specific vectorization service
   * configuration provided by the user. It also ensures the specified vector dimension complies
   * with config limits.
   *
   * @param vector The vector search configuration provided by the user in the create collection
   *     command.
   * @return The validated and potentially modified (adding default vector dimension) vector search
   *     configuration.
   * @throws APIException If vector search is disabled globally or the user configuration is
   *     invalid.
   */
  @VisibleForTesting
  public CreateCollectionCommand.Options.VectorSearchDesc validateVectorOptions(
      CreateCollectionCommand.Options.VectorSearchDesc vector) {

    if (vector.vectorizeConfig() != null && !operationsConfig.vectorizeEnabled()) {
      throw SchemaException.Code.VECTORIZE_FEATURE_NOT_AVAILABLE.get();
    }

    Integer vectorDimension = vector.dimension();
    VectorizeConfig service = vector.vectorizeConfig();
    String sourceModel = vector.sourceModel();
    String metric = vector.metric();

    // decide sourceModel and metric value
    if (sourceModel != null) {
      if (metric == null) {
        // (1) sourceModel is provided but metric is not - set metric to cosine or dot_product based
        // on the map
        // TODO: HAZEL this says ^^ "cosine or dot_product based on the map" but this is just the
        // default for model
        final String sourceModelFromUser = sourceModel;
        metric =
            EmbeddingSourceModel.fromApiNameOrDefault(sourceModelFromUser)
                .orElseThrow(
                    () -> EmbeddingSourceModel.getUnknownSourceModelException(sourceModelFromUser))
                .similarityFunction()
                .apiName();
      }
      // (2) both sourceModel and metric are provided - do nothing
    } else {
      if (metric != null) {
        // (3) sourceModel is not provided but metric is - set sourceModel to 'other'
        sourceModel = EmbeddingSourceModel.OTHER.cqlName();
      } else {
        // (4) both sourceModel and metric are not provided - set sourceModel to 'other' and metric
        // to 'cosine'
        sourceModel = EmbeddingSourceModel.DEFAULT.cqlName();
        metric = SimilarityFunction.DEFAULT.cqlIndexingFunction();
      }
    }

    if (service != null) {
      // Validate service configuration and auto populate vector dimension.
      vectorDimension = validateVectorize.validateService(service, vectorDimension);
      vector =
          new CreateCollectionCommand.Options.VectorSearchDesc(
              vectorDimension, metric, sourceModel, vector.vectorizeConfig());
    } else {
      // Ensure vector dimension is provided when service configuration is absent.
      if (vectorDimension == null) {
        throw SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS.get(
            "message", "The 'dimension' can not be null if 'service' is not provided");
      }
      if (vectorDimension > documentLimitsConfig.maxVectorEmbeddingLength()) {
        throw SchemaException.Code.VECTOR_SEARCH_TOO_BIG_VALUE.get(
            Map.of(
                "length",
                String.valueOf(vectorDimension),
                "maxLength",
                String.valueOf(documentLimitsConfig.maxVectorEmbeddingLength())));
      }
      vector =
          new CreateCollectionCommand.Options.VectorSearchDesc(
              vectorDimension, metric, sourceModel, null);
    }
    return vector;
  }
}
