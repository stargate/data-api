package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.PropertyBasedEmbeddingProviderConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.math.BigDecimal;

@ApplicationScoped
public class CreateCollectionCommandResolver implements CommandResolver<CreateCollectionCommand> {

  private final ObjectMapper objectMapper;
  private final CQLSessionCache cqlSessionCache;
  private final DocumentLimitsConfig documentLimitsConfig;
  private final DatabaseLimitsConfig dbLimitsConfig;
  private final OperationsConfig operationsConfig;
  private final PropertyBasedEmbeddingProviderConfig embeddingProviderConfig;

  @Inject
  public CreateCollectionCommandResolver(
      ObjectMapper objectMapper,
      CQLSessionCache cqlSessionCache,
      DocumentLimitsConfig documentLimitsConfig,
      DatabaseLimitsConfig dbLimitsConfig,
      OperationsConfig operationsConfig,
      PropertyBasedEmbeddingProviderConfig embeddingProviderConfig) {
    this.objectMapper = objectMapper;
    this.cqlSessionCache = cqlSessionCache;
    this.documentLimitsConfig = documentLimitsConfig;
    this.dbLimitsConfig = dbLimitsConfig;
    this.operationsConfig = operationsConfig;
    this.embeddingProviderConfig = embeddingProviderConfig;
  }

  public CreateCollectionCommandResolver() {
    this(null, null, null, null, null, null);
  }

  @Override
  public Class<CreateCollectionCommand> getCommandClass() {
    return CreateCollectionCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, CreateCollectionCommand command) {

    if (command.options() != null) {
      boolean vector = false;
      boolean indexing = false;
      Integer vectorDimension = null;
      String function = null;
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig service = null;

      // handling indexing options
      if (command.options().indexing() != null) {
        // validation of configuration
        command.options().indexing().validate();
        indexing = true;
        // No need to process if both are null or empty
      }

      // handling vector option
      if (command.options().vector() != null) {
        function = command.options().vector().metric();
        vectorDimension = command.options().vector().dimension();
        service = command.options().vector().vectorizeConfig();

        if (service != null) {
          // Validate the user input service config. If no error, the method will return the
          // dimension in config file
          vectorDimension = validateService(service, vectorDimension);
        } else {
          // if the service is not provided, the vector dimension cannot be null
          if (vectorDimension == null) {
            throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
                "The dimensions can not be null if service is not provided");
          }
        }
        // TODO: what if the config model vector dimension is larger than 4096?
        if (vectorDimension > documentLimitsConfig.maxVectorEmbeddingLength()) {
          throw new JsonApiException(
              ErrorCode.VECTOR_SEARCH_TOO_BIG_VALUE,
              String.format(
                  "%s: %d (max %d)",
                  ErrorCode.VECTOR_SEARCH_TOO_BIG_VALUE.getMessage(),
                  vectorDimension,
                  documentLimitsConfig.maxVectorEmbeddingLength()));
        }
        vector = true;
      }

      String comment = null;
      if (indexing || vector) {
        final ObjectNode collectionNode = objectMapper.createObjectNode();
        ObjectNode optionsNode =
            objectMapper.createObjectNode(); // Create a new ObjectNode for collection options
        if (indexing) {
          optionsNode.putPOJO(
              TableCommentConstants.COLLECTION_INDEXING_KEY, command.options().indexing());
        }
        if (vector) {
          CreateCollectionCommand.Options.VectorSearchConfig newVector =
              new CreateCollectionCommand.Options.VectorSearchConfig(
                  vectorDimension,
                  command.options().vector().metric(),
                  command.options().vector().vectorizeConfig());
          optionsNode.putPOJO(TableCommentConstants.COLLECTION_VECTOR_KEY, newVector);
        }
        collectionNode.put(TableCommentConstants.COLLECTION_NAME_KEY, command.name());
        collectionNode.put(
            TableCommentConstants.SCHEMA_VERSION_KEY, TableCommentConstants.SCHEMA_VERSION_VALUE);
        collectionNode.putPOJO(TableCommentConstants.OPTIONS_KEY, optionsNode);
        final ObjectNode tableCommentNode = objectMapper.createObjectNode();
        tableCommentNode.putPOJO(TableCommentConstants.TOP_LEVEL_KEY, collectionNode);
        comment = tableCommentNode.toString();
      }

      if (command.options().vector() != null) {
        return CreateCollectionOperation.withVectorSearch(
            ctx,
            dbLimitsConfig,
            objectMapper,
            cqlSessionCache,
            command.name(),
            vectorDimension,
            function,
            comment,
            operationsConfig.databaseConfig().ddlDelayMillis());
      } else {
        return CreateCollectionOperation.withoutVectorSearch(
            ctx,
            dbLimitsConfig,
            objectMapper,
            cqlSessionCache,
            command.name(),
            comment,
            operationsConfig.databaseConfig().ddlDelayMillis());
      }
    } else {
      return CreateCollectionOperation.withoutVectorSearch(
          ctx,
          dbLimitsConfig,
          objectMapper,
          cqlSessionCache,
          command.name(),
          null,
          operationsConfig.databaseConfig().ddlDelayMillis());
    }
  }

  /**
   * Validates the user-provided service configuration against internal configurations. It checks
   * for the existence and enabled status of the service provider, the necessity of secret names for
   * certain authentication types, the validity of provided parameters against expected types, and
   * the appropriateness of model dimensions. It ensures that all required and type-specific
   * conditions are met for the service to be considered valid.
   *
   * @param userConfig The user input vectorize service configuration.
   * @param userVectorDimension The dimension specified by the user, may be null.
   * @return The dimension to be used for the vector, should be from the internal configuration. It
   *     will be used for auto populate the vector dimension
   * @throws JsonApiException If the service configuration is invalid or unsupported.
   */
  private Integer validateService(
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig userConfig,
      Integer userVectorDimension) {
    // Check if the service provider exists and is enabled
    PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig providerConfig =
        getAndValidateProviderConfig(userConfig);

    // Check secret name for shared secret authentication, if applicable
    validateSecretNameForSharedSecret(userConfig);

    // Validate user-provided parameters against internal expectations
    validateUserParameters(userConfig, providerConfig);

    // Validate the model and its vector dimension
    return validateModelAndDimension(userConfig, providerConfig, userVectorDimension);
  }

  private PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig getAndValidateProviderConfig(
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig userConfig) {
    PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig providerConfig =
        embeddingProviderConfig.providers().get(userConfig.provider());
    if (providerConfig == null || !providerConfig.enabled()) {
      throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Service provider '%s' is not supported.", userConfig.provider());
    }
    return providerConfig;
  }

  private void validateSecretNameForSharedSecret(
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig userConfig) {
    if (userConfig.vectorizeServiceAuthentication().type().contains("SHARED_SECRET")
        && userConfig.vectorizeServiceAuthentication().secretName() == null) {
      throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'secret_name' must be provided for 'SHARED_SECRET' authentication type.");
    }
  }

  // TODO: what if user provide some useless parameters (i.e. not in config), just ignore?
  private void validateUserParameters(
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig userConfig,
      PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig providerConfig) {
    if (providerConfig.parameters() == null) return;

    providerConfig
        .parameters()
        .forEach(
            expectedParamConfig -> {
              Object userParamValue =
                  userConfig.vectorizeServiceParameter().get(expectedParamConfig.name());
              // if the user doesn't provide the parameters in config
              if (userParamValue == null) {
                // if the parameter is required, throw error
                if (expectedParamConfig.required()) {
                  throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
                      "Please provide required parameter '%s'.", expectedParamConfig.name());
                }
              } else {
                // if the user provides the parameters in config, validate the parameter type
                validateParameterType(expectedParamConfig, userParamValue);
              }
            });
  }

  private void validateParameterType(
      PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig.ParameterConfig
          expectedParamConfig,
      Object userParamValue) {

    PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig.ParameterType expectedParamType =
        expectedParamConfig.type();
    boolean typeMismatch =
        expectedParamType
                    == PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig.ParameterType
                        .STRING
                && !(userParamValue instanceof String)
            || expectedParamType
                    == PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig.ParameterType
                        .NUMBER
                && !(userParamValue instanceof BigDecimal)
            || expectedParamType
                    == PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig.ParameterType
                        .BOOLEAN
                && !(userParamValue instanceof Boolean);

    if (typeMismatch) {
      throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "The provided parameter (%s) type is incorrect. Expected: '%s'",
          expectedParamConfig.name(), expectedParamType);
    }
  }

  // TODO: check model parameters provided by the user, will support in the future
  private Integer validateModelAndDimension(
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig userConfig,
      PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig providerConfig,
      Integer userVectorDimension) {
    PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig.ModelConfig model =
        providerConfig.models().stream()
            .filter(m -> m.name().equals(userConfig.modelName()))
            .findFirst()
            .orElseThrow(
                () ->
                    ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
                        "Model name '%s' for provider '%s' is not supported.",
                        userConfig.modelName(), userConfig.provider()));

    Integer configVectorDimension = Integer.parseInt(model.properties().get("vector-dimension"));
    if (userVectorDimension == null) {
      return configVectorDimension; // Use config dimension if user didn't provide one
    } else if (!configVectorDimension.equals(userVectorDimension)) {
      throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "The provided dimension value '%s' doesn't match the model supports dimension value '%s'.",
          userVectorDimension, configVectorDimension);
    }
    return configVectorDimension;
  }
}
