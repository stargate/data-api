package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.api.common.config.DataStoreConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class CreateCollectionCommandResolver implements CommandResolver<CreateCollectionCommand> {

  private final ObjectMapper objectMapper;
  private final CQLSessionCache cqlSessionCache;
  private final DataStoreConfig dataStoreConfig;
  private final DocumentLimitsConfig documentLimitsConfig;
  private final DatabaseLimitsConfig dbLimitsConfig;
  private final OperationsConfig operationsConfig;
  private final EmbeddingProvidersConfig embeddingProvidersConfig;
  private final ValidateCredentials validateCredentials;

  @Inject
  public CreateCollectionCommandResolver(
      ObjectMapper objectMapper,
      CQLSessionCache cqlSessionCache,
      DataStoreConfig dataStoreConfig,
      DocumentLimitsConfig documentLimitsConfig,
      DatabaseLimitsConfig dbLimitsConfig,
      OperationsConfig operationsConfig,
      EmbeddingProvidersConfig embeddingProvidersConfig,
      ValidateCredentials validateCredentials) {
    this.objectMapper = objectMapper;
    this.cqlSessionCache = cqlSessionCache;
    this.dataStoreConfig = dataStoreConfig;
    this.documentLimitsConfig = documentLimitsConfig;
    this.dbLimitsConfig = dbLimitsConfig;
    this.operationsConfig = operationsConfig;
    this.embeddingProvidersConfig = embeddingProvidersConfig;
    this.validateCredentials = validateCredentials;
  }

  public CreateCollectionCommandResolver() {
    this(null, null, null, null, null, null, null, null);
  }

  @Override
  public Class<CreateCollectionCommand> getCommandClass() {
    return CreateCollectionCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, CreateCollectionCommand command) {
    if (command.options() == null) {
      return CreateCollectionOperation.withoutVectorSearch(
          ctx,
          dbLimitsConfig,
          objectMapper,
          cqlSessionCache,
          command.name(),
          generateComment(objectMapper, false, false, command.name(), null, null, null),
          operationsConfig.databaseConfig().ddlDelayMillis(),
          operationsConfig.tooManyIndexesRollbackEnabled(),
          false); // Since the options is null
    }

    boolean hasIndexing = command.options().indexing() != null;
    boolean hasVectorSearch = command.options().vector() != null;
    CreateCollectionCommand.Options.VectorSearchConfig vector = command.options().vector();

    boolean indexingDenyAll = false;
    // handling indexing options
    if (hasIndexing) {
      // validation of configuration
      command.options().indexing().validate();
      indexingDenyAll = command.options().indexing().denyAll();
      // No need to process if both are null or empty
    }

    // handling vector option
    if (hasVectorSearch) {
      vector = validateVectorOptions(vector);
    }

    String comment =
        generateComment(
            objectMapper,
            hasIndexing,
            hasVectorSearch,
            command.name(),
            command.options().indexing(),
            vector,
            command.options().idConfig());

    if (hasVectorSearch) {
      return CreateCollectionOperation.withVectorSearch(
          ctx,
          dbLimitsConfig,
          objectMapper,
          cqlSessionCache,
          command.name(),
          vector.dimension(),
          vector.metric(),
          comment,
          operationsConfig.databaseConfig().ddlDelayMillis(),
          operationsConfig.tooManyIndexesRollbackEnabled(),
          indexingDenyAll);
    } else {
      return CreateCollectionOperation.withoutVectorSearch(
          ctx,
          dbLimitsConfig,
          objectMapper,
          cqlSessionCache,
          command.name(),
          comment,
          operationsConfig.databaseConfig().ddlDelayMillis(),
          operationsConfig.tooManyIndexesRollbackEnabled(),
          indexingDenyAll);
    }
  }

  /**
   * Generate a JSON string comment that will be stored in the database.
   *
   * @param hasIndexing indicating if indexing options are enabled.
   * @param hasVectorSearch indicating if vector search options are enabled.
   * @param commandName command name
   * @param indexing the indexing option config
   * @param vector vector config after validation
   * @return the comment string
   */
  public static String generateComment(
      ObjectMapper objectMapper,
      boolean hasIndexing,
      boolean hasVectorSearch,
      String commandName,
      CreateCollectionCommand.Options.IndexingConfig indexing,
      CreateCollectionCommand.Options.VectorSearchConfig vector,
      CreateCollectionCommand.Options.IdConfig idConfig) {
    final ObjectNode collectionNode = objectMapper.createObjectNode();
    ObjectNode optionsNode = objectMapper.createObjectNode(); // For storing collection options.

    if (hasIndexing) {
      optionsNode.putPOJO(TableCommentConstants.COLLECTION_INDEXING_KEY, indexing);
    }
    if (hasVectorSearch) {
      optionsNode.putPOJO(TableCommentConstants.COLLECTION_VECTOR_KEY, vector);
    }
    // if default_id is not specified during createCollection, resolve type to empty string
    if (idConfig != null) {
      optionsNode.putPOJO(TableCommentConstants.DEFAULT_ID_KEY, idConfig);
    } else {
      optionsNode.putPOJO(
          TableCommentConstants.DEFAULT_ID_KEY,
          objectMapper.createObjectNode().putPOJO("type", ""));
    }

    collectionNode.put(TableCommentConstants.COLLECTION_NAME_KEY, commandName);
    collectionNode.put(
        TableCommentConstants.SCHEMA_VERSION_KEY, TableCommentConstants.SCHEMA_VERSION_VALUE);
    collectionNode.putPOJO(TableCommentConstants.OPTIONS_KEY, optionsNode);
    final ObjectNode tableCommentNode = objectMapper.createObjectNode();
    tableCommentNode.putPOJO(TableCommentConstants.TOP_LEVEL_KEY, collectionNode);
    return tableCommentNode.toString();
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
   * @throws JsonApiException If vector search is disabled globally or the user configuration is
   *     invalid.
   */
  private CreateCollectionCommand.Options.VectorSearchConfig validateVectorOptions(
      CreateCollectionCommand.Options.VectorSearchConfig vector) {
    if (!dataStoreConfig.vectorSearchEnabled()) {
      throw new JsonApiException(
          ErrorCode.VECTOR_SEARCH_NOT_AVAILABLE,
          ErrorCode.VECTOR_SEARCH_NOT_AVAILABLE.getMessage());
    }

    if (vector.vectorizeConfig() != null && !operationsConfig.vectorizeEnabled()) {
      throw ErrorCode.VECTORIZE_FEATURE_NOT_AVAILABLE.toApiException();
    }

    Integer vectorDimension = vector.dimension();
    CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig service =
        vector.vectorizeConfig();

    if (service != null) {
      // Validate service configuration and auto populate vector dimension.
      vectorDimension = validateService(service, vectorDimension);
      vector =
          new CreateCollectionCommand.Options.VectorSearchConfig(
              vectorDimension, vector.metric(), vector.vectorizeConfig());
    } else {
      // Ensure vector dimension is provided when service configuration is absent.
      if (vectorDimension == null) {
        throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
            "The 'dimension' can not be null if 'service' is not provided");
      }
      if (vectorDimension > documentLimitsConfig.maxVectorEmbeddingLength()) {
        throw new JsonApiException(
            ErrorCode.VECTOR_SEARCH_TOO_BIG_VALUE,
            String.format(
                "%s: %d (max %d)",
                ErrorCode.VECTOR_SEARCH_TOO_BIG_VALUE.getMessage(),
                vectorDimension,
                documentLimitsConfig.maxVectorEmbeddingLength()));
      }
    }
    return vector;
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
    // Only for internal tests
    if (userConfig.provider().equals(ProviderConstants.CUSTOM)) {
      return userVectorDimension;
    }
    // Check if the service provider exists and is enabled
    EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig =
        getAndValidateProviderConfig(userConfig);

    // Check secret name for shared secret authentication, if applicable
    validateAuthentication(userConfig, providerConfig);

    // Validate the model and its vector dimension:
    //   huggingFaceDedicated: must have vectorDimension specified
    //   other providers: must have model specified, and default dimension when dimension not
    // specified
    Integer vectorDimension =
        validateModelAndDimension(userConfig, providerConfig, userVectorDimension);

    // Validate user-provided parameters against internal expectations
    validateUserParameters(userConfig, providerConfig);

    return vectorDimension;
  }

  /**
   * Retrieves and validates the provider configuration for vector search based on user input. This
   * method ensures that the specified service provider is configured and enabled in the system.
   *
   * @param userConfig The configuration provided by the user specifying the vector search provider.
   * @return The configuration for the embedding provider, if valid.
   * @throws JsonApiException If the provider is not supported or not enabled.
   */
  private EmbeddingProvidersConfig.EmbeddingProviderConfig getAndValidateProviderConfig(
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig userConfig) {
    EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig =
        embeddingProvidersConfig.providers().get(userConfig.provider());
    if (providerConfig == null || !providerConfig.enabled()) {
      throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Service provider '%s' is not supported", userConfig.provider());
    }
    return providerConfig;
  }

  /**
   * Validates user authentication for creating a collection using the specified configurations.
   *
   * @param userConfig The vectorize configuration provided by the user.
   * @param providerConfig The embedding provider configuration.
   * @throws JsonApiException If the user authentication is invalid.
   */
  private void validateAuthentication(
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig userConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {
    // Get all the accepted keys in auth
    List<String> acceptedKeys =
        providerConfig.supportedAuthentications().values().stream()
            .filter(config -> config.enabled() && config.tokens() != null)
            .flatMap(config -> config.tokens().stream())
            .map(EmbeddingProvidersConfig.EmbeddingProviderConfig.TokenConfig::accepted)
            .toList();

    // If the user hasn't provided authentication details, verify that either the 'NONE' or 'HEADER'
    // authentication type is enabled.
    if (userConfig.authentication() == null || userConfig.authentication().isEmpty()) {
      EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig noneAuthConfig =
          providerConfig
              .supportedAuthentications()
              .get(EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType.NONE);
      EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig headerAuthConfig =
          providerConfig
              .supportedAuthentications()
              .get(EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType.HEADER);

      // Check if either 'NONE' or 'HEADER' authentication type is enabled
      boolean noneEnabled = (noneAuthConfig != null && noneAuthConfig.enabled());
      boolean headerEnabled = (headerAuthConfig != null && headerAuthConfig.enabled());
      if (!noneEnabled && !headerEnabled) {
        throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
            "Service provider '%s' does not support either 'NONE' or 'HEADER' authentication types.",
            userConfig.provider());
      }
    } else {
      // User has provided authentication details. Validate each key against the provider's accepted
      // list.
      for (Map.Entry<String, String> userAuth : userConfig.authentication().entrySet()) {
        if (!acceptedKeys.contains(userAuth.getKey())) {
          throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
              "Service provider '%s' does not support authentication key '%s'",
              userConfig.provider(), userAuth.getKey());
        } else {
          if (userAuth.getKey().equals("providerKey")) {
            // sharedKeyValue must be in the format of "keyName.providerKey"
            String sharedKeyValue = userAuth.getValue();
            if (sharedKeyValue == null || sharedKeyValue.isEmpty()) {
              throw ErrorCode.VECTORIZE_INVALID_SHARED_KEY_VALUE_FORMAT.toApiException(
                  "missing value");
            }
            int dotIndex = sharedKeyValue.lastIndexOf('.');
            if (dotIndex <= 0) {
              throw ErrorCode.VECTORIZE_INVALID_SHARED_KEY_VALUE_FORMAT.toApiException(
                  "providerKey value should be formatted as '[keyName].providerKey'");
            }

            String providerKeyString = sharedKeyValue.substring(dotIndex + 1);
            if (!"providerKey".equals(providerKeyString)) {
              throw ErrorCode.VECTORIZE_INVALID_SHARED_KEY_VALUE_FORMAT.toApiException(
                  "providerKey value should be formatted as '[keyName].providerKey'");
            }
          }
          if (operationsConfig.enableEmbeddingGateway()) {
            validateCredentials.validate(userConfig.provider(), userAuth.getValue());
          }
        }
      }
    }
  }

  /**
   * Validates the parameters provided by the user against the expected parameters from both the
   * provider and the model configurations. This method ensures that only configured parameters are
   * provided, all required parameters are included, and no unexpected parameters are passed.
   *
   * @param userConfig The vector search configuration provided by the user.
   * @param providerConfig The configuration of the embedding provider which includes model and
   *     provider-level parameters.
   * @throws JsonApiException if any unconfigured parameters are provided, required parameters are
   *     missing, or if an error occurs due to no parameters being configured but some are provided
   *     by the user.
   */
  private void validateUserParameters(
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig userConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {
    // 0. Combine provider level and model level parameters
    List<EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig> allParameters =
        new ArrayList<>();
    // Add all provider level parameters
    allParameters.addAll(providerConfig.parameters());
    // Get all the parameters except "vectorDimension" for the model -- model has been validated in
    // the previous step, huggingfaceDedicated does not have model specified
    if (!userConfig.provider().equals(ProviderConstants.HUGGINGFACE_DEDICATED)) {
      List<EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig> modelParameters =
          providerConfig.models().stream()
              .filter(m -> m.name().equals(userConfig.modelName()))
              .findFirst()
              .map(EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig::parameters)
              .map(
                  params ->
                      params.stream()
                          .filter(
                              param ->
                                  !param
                                      .name()
                                      .equals(
                                          "vectorDimension")) // Exclude 'vectorDimension' parameter
                          .collect(Collectors.toList()))
              .get();
      // Add all model level parameters
      allParameters.addAll(modelParameters);
    }
    // 1. Error if the user provided un-configured parameters
    // Two level parameters have unique names, should be fine here
    Set<String> expectedParamNames =
        allParameters.stream()
            .map(EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig::name)
            .collect(Collectors.toSet());
    Map<String, Object> userParameters =
        (userConfig.parameters() != null) ? userConfig.parameters() : Collections.emptyMap();
    // Check for unconfigured parameters provided by the user
    userParameters
        .keySet()
        .forEach(
            userParamName -> {
              if (!expectedParamNames.contains(userParamName)) {
                throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
                    "Unexpected parameter '%s' for the provider '%s' provided",
                    userParamName, userConfig.provider());
              }
            });

    // 2. Error if the user doesn't provide required parameters
    // Check for missing required parameters and collect them for type validation
    List<EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig> parametersToValidate =
        new ArrayList<>();
    allParameters.forEach(
        expectedParamConfig -> {
          if (expectedParamConfig.required()
              && !userParameters.containsKey(expectedParamConfig.name())) {
            throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
                "Required parameter '%s' for the provider '%s' missing",
                expectedParamConfig.name(), userConfig.provider());
          }
          if (userParameters.containsKey(expectedParamConfig.name())) {
            parametersToValidate.add(expectedParamConfig);
          }
        });

    // 3. Validate parameter types if no errors occurred in previous steps
    parametersToValidate.forEach(
        expectedParamConfig ->
            validateParameterType(
                expectedParamConfig, userParameters.get(expectedParamConfig.name())));
  }

  /**
   * Validates the type of parameter provided by the user against the expected type defined in the
   * provider's configuration. This method checks if the type of the user-provided parameter matches
   * the expected type, throwing an exception if there is a mismatch.
   *
   * @param expectedParamConfig The expected configuration for the parameter which includes its
   *     expected type.
   * @param userParamValue The value of the parameter provided by the user.
   * @throws JsonApiException if the type of the parameter provided by the user does not match the
   *     expected type.
   */
  private void validateParameterType(
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig expectedParamConfig,
      Object userParamValue) {

    EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterType expectedParamType =
        expectedParamConfig.type();
    boolean typeMismatch =
        expectedParamType == EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterType.STRING
                && !(userParamValue instanceof String)
            || expectedParamType
                    == EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterType.NUMBER
                && !(userParamValue instanceof Number)
            || expectedParamType
                    == EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterType.BOOLEAN
                && !(userParamValue instanceof Boolean);

    if (typeMismatch) {
      throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "The provided parameter '%s' type is incorrect. Expected: '%s'",
          expectedParamConfig.name(), expectedParamType);
    }
  }

  /**
   * Validates the model name and vector dimension provided in the user configuration against the
   * specified embedding provider configuration.
   *
   * @param userConfig the user-specified vectorization configuration
   * @param providerConfig the configuration of the embedding provider
   * @param userVectorDimension the vector dimension provided by the user, or null if not provided
   * @return the validated vector dimension to be used for the model
   * @throws JsonApiException if the model name is not found, or if the dimension is invalid
   */
  private Integer validateModelAndDimension(
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig userConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      Integer userVectorDimension) {
    // Find the model configuration by matching the model name
    // 1. huggingfaceDedicated does not require model, but requires dimension
    if (userConfig.provider().equals(ProviderConstants.HUGGINGFACE_DEDICATED)) {
      if (userVectorDimension == null) {
        throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
            "dimension is needed for huggingfaceDedicated provider");
      }
      // validate the numeric-range for user-input dimension
      validateRangeDimension(
          Objects.requireNonNull(providerConfig.parameters()).stream()
              .filter(param -> param.name().equals("vectorDimension"))
              .findFirst()
              .get(),
          userVectorDimension);
      return userVectorDimension;
    }
    // 2. other providers do require model
    if (userConfig.modelName() == null) {
      throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "model can not be null for provider '%s'", userConfig.provider());
    }
    EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model =
        providerConfig.models().stream()
            .filter(m -> m.name().equals(userConfig.modelName()))
            .findFirst()
            .orElseThrow(
                () ->
                    ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
                        "Model name '%s' for provider '%s' is not supported",
                        userConfig.modelName(), userConfig.provider()));

    // Handle models with a fixed vector dimension
    if (model.vectorDimension().isPresent() && model.vectorDimension().get() != 0) {
      Integer configVectorDimension = model.vectorDimension().get();
      if (userVectorDimension == null) {
        return configVectorDimension; // Use model's dimension if user hasn't specified any
      } else if (!configVectorDimension.equals(userVectorDimension)) {
        throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
            "The provided dimension value '%s' doesn't match the model's supported dimension value '%s'",
            userVectorDimension, configVectorDimension);
      }
      return configVectorDimension;
    }

    // Handle models with a range of acceptable dimensions
    return model.parameters().stream()
        .filter(param -> param.name().equals("vectorDimension"))
        .findFirst()
        .map(param -> validateRangeDimension(param, userVectorDimension))
        .orElse(userVectorDimension); // should not go here
  }

  /**
   * Validates the user-provided vector dimension against the dimension parameter's validation
   * constraints.
   *
   * @param param the parameter configuration containing validation constraints
   * @param userVectorDimension the vector dimension provided by the user
   * @return the appropriate vector dimension based on parameter configuration
   * @throws JsonApiException if the user-provided dimension is not valid
   */
  private Integer validateRangeDimension(
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig param,
      Integer userVectorDimension) {
    // Use the default value if the user has not provided a dimension
    if (userVectorDimension == null) {
      return Integer.valueOf(param.defaultValue().get());
    }

    // Extract validation type and values for comparison
    Map.Entry<EmbeddingProvidersConfig.EmbeddingProviderConfig.ValidationType, List<Integer>>
        entry = param.validation().entrySet().iterator().next();
    EmbeddingProvidersConfig.EmbeddingProviderConfig.ValidationType validationType = entry.getKey();
    List<Integer> validationValues = entry.getValue();

    // Perform validation based on the validation type
    switch (validationType) {
      case NUMERIC_RANGE -> {
        if (userVectorDimension < validationValues.get(0)
            || userVectorDimension > validationValues.get(1)) {
          throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
              "The provided dimension value (%d) is not within the supported numeric range [%d, %d]",
              userVectorDimension, validationValues.get(0), validationValues.get(1));
        }
      }
      case OPTIONS -> {
        if (!validationValues.contains(userVectorDimension)) {
          String validatedValuesStr =
              String.join(
                  ", ", validationValues.stream().map(Object::toString).toArray(String[]::new));
          throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
              "The provided dimension value '%s' is not within the supported options [%s]",
              userVectorDimension, validatedValuesStr);
        }
      }
    }
    return userVectorDimension;
  }
}
