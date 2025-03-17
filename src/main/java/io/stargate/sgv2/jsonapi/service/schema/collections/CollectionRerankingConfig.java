package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.constants.RerankingConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import java.util.*;
import java.util.stream.Collectors;

/** Validated configuration Object for Reranking configuration for Collections. */
public record CollectionRerankingConfig(
    boolean enabled,
    @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty("service")
        RerankingProviderConfig rerankingProviderConfig) {

  // Create a nested record for the provider-related fields
  public record RerankingProviderConfig(
      String provider,
      String modelName,
      Map<String, String> authentication,
      Map<String, Object> parameters) {}

  public CollectionRerankingConfig(
      boolean enabled,
      String provider,
      String modelName,
      Map<String, String> authentication,
      Map<String, Object> parameters) {
    // Clear out any reranking settings if not enabled (but don't fail)
    this(
        enabled,
        enabled
            ? new RerankingProviderConfig(provider, modelName, authentication, parameters)
            : null);
  }

  /**
   * Accessor for an instance to use for a default configuration for newly created collections:
   * where no configuration defined: needs to be enabled, using "nvidia" reranking service
   * configuration.
   */
  public static CollectionRerankingConfig configForNewCollections(
      RerankingProvidersConfig rerankingProvidersConfig) {
    // get the default provider from the config
    var defaultProvider =
        rerankingProvidersConfig.providers().entrySet().stream()
            .filter(entry -> entry.getValue().isDefault())
            .findFirst();
    if (defaultProvider.isEmpty()) {
      return new CollectionRerankingConfig(false, null);
    }
    var provider = defaultProvider.get().getKey();
    var modelName =
        rerankingProvidersConfig.providers().get(provider).models().stream()
            .filter(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig::isDefault)
            .findFirst()
            .map(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig::name)
            .get();

    // TODO(Hazel): Assume no authentication or parameters for default provider and model now, may
    // need to change
    var defaultRerankingService = new RerankingProviderConfig(provider, modelName, null, null);

    return new CollectionRerankingConfig(true, defaultRerankingService);
  }

  /**
   * Accessor for an instance to use for existing pre-reranking collections: ones without reranking
   * field and index: needs to be disabled
   */
  public static CollectionRerankingConfig configForLegacyCollections() {
    return new CollectionRerankingConfig(false, null);
  }

  /**
   * Accessor for an instance to use for missing collection: cases where definition does not exist:
   * needs to be disabled.
   */
  public static CollectionRerankingConfig configForMissingCollection() {
    return new CollectionRerankingConfig(false, null);
  }

  /** Read the reranking configuration from the JSON node. */
  public static CollectionRerankingConfig fromJson(
      JsonNode rerankingJsonNode, ObjectMapper objectMapper) {
    boolean enabled =
        rerankingJsonNode.path(RerankingConstants.RerankingColumn.ENABLED).asBoolean(false);

    if (!enabled) {
      return new CollectionRerankingConfig(enabled, null);
    }

    JsonNode rerankingServiceNode =
        rerankingJsonNode.get(RerankingConstants.RerankingColumn.SERVICE);

    // provider, modelName, must exist
    // TODO: WHAT HAPPENS IF THEY DONT ? JSON props on VectorizeConfig say model is not
    // required
    String provider =
        rerankingServiceNode.get(RerankingConstants.RerankingService.PROVIDER).asText();
    String modelName =
        rerankingServiceNode.get(RerankingConstants.RerankingService.MODEL_NAME).asText();

    // construct VectorizeDefinition.authentication, can be null
    JsonNode authNode =
        rerankingServiceNode.get(RerankingConstants.RerankingService.AUTHENTICATION);
    // TODO: remove unchecked assignment
    Map<String, String> authMap =
        authNode == null ? null : objectMapper.convertValue(authNode, Map.class);

    // construct VectorizeDefinition.parameters, can be null
    JsonNode paramsNode = rerankingServiceNode.get(RerankingConstants.RerankingService.PARAMETERS);
    // TODO: remove unchecked assignment
    Map<String, Object> paramsMap =
        paramsNode == null ? null : objectMapper.convertValue(paramsNode, Map.class);

    return new CollectionRerankingConfig(enabled, provider, modelName, authMap, paramsMap);
  }

  /**
   * Method for validating the reranking config passed and constructing actual configuration object
   * to use.
   *
   * @return Valid CollectionRerankingConfig object
   */
  public static CollectionRerankingConfig validateAndConstruct(
      CreateCollectionCommand.Options.RerankingConfigDefinition rerankingConfig,
      RerankingProvidersConfig rerankingProvidersConfig) {
    // If not defined, use default for new collections; valid option
    if (rerankingConfig == null) {
      return configForNewCollections(rerankingProvidersConfig);
    }
    // Otherwise validate and construct
    Boolean enabled = rerankingConfig.enabled();
    if (enabled == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'enabled' is required property for 'rerank' Object value");
    }
    // If not enabled, clear out any reranking settings (but don't fail)
    if (!enabled) {
      return new CollectionRerankingConfig(enabled, null);
    }

    // If enabled, but no service config, use default
    var rerankingServiceConfig = rerankingConfig.rerankingServiceConfig();
    if (rerankingServiceConfig == null) {
      return configForNewCollections(rerankingProvidersConfig);
    }

    String provider = rerankingConfig.rerankingServiceConfig().provider();
    String modelName = rerankingConfig.rerankingServiceConfig().modelName();
    Map<String, String> authentication = rerankingConfig.rerankingServiceConfig().authentication();
    Map<String, Object> parameters = rerankingConfig.rerankingServiceConfig().parameters();

    RerankingProvidersConfig.RerankingProviderConfig providerConfig =
        getAndValidateProviderConfig(provider, rerankingProvidersConfig);

    modelName = validateModel(provider, modelName, providerConfig);

    authentication = validateAuthentication(provider, authentication, providerConfig);

    // TODO(Hazel): No need to validate the parameters, add back when it's needed

    return new CollectionRerankingConfig(enabled, provider, modelName, authentication, parameters);
  }

  /**
   * Retrieves and validates the provider configuration for reranking model based on user input.
   * This method ensures that the specified service provider is configured and enabled in the
   * system.
   *
   * @param provider The name of the service provider.
   * @param rerankingProvidersConfig The configuration for available reranking providers.
   * @return The configuration for the reranking provider, if valid.
   * @throws JsonApiException If the provider is not supported or not enabled.
   */
  private static RerankingProvidersConfig.RerankingProviderConfig getAndValidateProviderConfig(
      String provider, RerankingProvidersConfig rerankingProvidersConfig) {
    if (provider == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'provider' is required property for 'rerank.service' Object value");
    }

    var providerConfig = rerankingProvidersConfig.providers().get(provider);
    if (providerConfig == null || !providerConfig.enabled()) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Reranking provider '%s' is not supported", provider);
    }
    return providerConfig;
  }

  /**
   * Validates the model name provided by the user against the specified reranking provider
   * configuration.
   */
  private static String validateModel(
      String provider,
      String modelName,
      RerankingProvidersConfig.RerankingProviderConfig rerankingProviderConfig) {
    // 1. model name is required
    if (modelName == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'modelName' is needed for reranking provider %s", provider);
    }
    // 2. model name must be supported by the provider - in the config
    rerankingProviderConfig.models().stream()
        .filter(m -> m.name().equals(modelName))
        .findFirst()
        .orElseThrow(
            () ->
                ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
                    "Model name '%s' for reranking provider '%s' is not supported",
                    modelName, provider));
    return modelName;
  }

  /**
   * Validates user authentication for reranking models when creating a collection using the
   * specified configurations.
   *
   * <ol>
   *   <li>Validate that all keys (member names) in the authentication stanza (e.g. providerKey) are
   *       listed in the configuration for the provider as accepted keys.
   *   <li>For each key-value member of the authentication stanza:
   *       <ol type="a">
   *         <li>If the value does not contain the period character "." it assumes the value is the
   *             name of the credential without specifying the key.
   *             <ol type="i">
   *               <li>The credential name is appended with .&lt;key&gt; and the secret service
   *                   called to validate that a credential with that name exists and it has the
   *                   named key.
   *             </ol>
   *         <li>If the value does contain a period character "." it assumes the first part is the
   *             name of the credential and the second the name of the key within it.
   *             <ol type="i">
   *               <li>The secret service called to validate that a credential with that name exists
   *                   and it has the named key.
   *             </ol>
   *       </ol>
   * </ol>
   *
   * @param provider The reranking provider name.
   * @param authentication The reranking authentication details.
   * @throws JsonApiException If the user authentication is invalid.
   */
  private static Map<String, String> validateAuthentication(
      String provider,
      Map<String, String> authentication,
      RerankingProvidersConfig.RerankingProviderConfig rerankingProviderConfig) {
    // Get all the accepted keys in SHARED_SECRET
    Set<String> acceptedKeys =
        rerankingProviderConfig.supportedAuthentications().entrySet().stream()
            .filter(
                config ->
                    config
                        .getKey()
                        .equals(
                            RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType
                                .SHARED_SECRET))
            .filter(config -> config.getValue().enabled() && config.getValue().tokens() != null)
            .flatMap(config -> config.getValue().tokens().stream())
            .map(RerankingProvidersConfig.RerankingProviderConfig.TokenConfig::accepted)
            .collect(Collectors.toSet());

    // If the user hasn't provided authentication details, verify that either the 'NONE' or 'HEADER'
    // authentication type is enabled.
    if (authentication == null || authentication.isEmpty()) {
      // Check if 'NONE' authentication type is enabled
      boolean noneEnabled =
          Optional.ofNullable(
                  rerankingProviderConfig
                      .supportedAuthentications()
                      .get(
                          RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType.NONE))
              .map(RerankingProvidersConfig.RerankingProviderConfig.AuthenticationConfig::enabled)
              .orElse(false);

      // Check if 'HEADER' authentication type is enabled
      boolean headerEnabled =
          Optional.ofNullable(
                  rerankingProviderConfig
                      .supportedAuthentications()
                      .get(
                          RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType
                              .HEADER))
              .map(RerankingProvidersConfig.RerankingProviderConfig.AuthenticationConfig::enabled)
              .orElse(false);

      // If neither 'NONE' nor 'HEADER' authentication type is enabled, throw an exception
      if (!noneEnabled && !headerEnabled) {
        throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
            "Reranking provider '%s' does not support either 'NONE' or 'HEADER' authentication types.",
            provider);
      }
    } else {
      // User has provided authentication details. Validate each key against the provider's accepted
      // list.

      Map<String, String> updatedAuth = new HashMap<>();
      for (Map.Entry<String, String> userAuth : authentication.entrySet()) {
        // Determine the full credential name based on the sharedKeyValue pair
        // If the sharedKeyValue does not contain a dot (e.g. myKey) or the part after the dot
        // does not match the key (e.g. myKey.test), append the key to the sharedKeyValue with
        // a dot (e.g. myKey.providerKey or myKey.test.providerKey). Otherwise, use the
        // sharedKeyValue (e.g. myKey.providerKey) as is.
        String sharedKeyValue = userAuth.getValue();
        String credentialName =
            sharedKeyValue.lastIndexOf('.') <= 0
                    || !sharedKeyValue
                        .substring(sharedKeyValue.lastIndexOf('.') + 1)
                        .equals(userAuth.getKey())
                ? sharedKeyValue + "." + userAuth.getKey()
                : sharedKeyValue;
        updatedAuth.put(userAuth.getKey(), credentialName);
        authentication = updatedAuth;
      }

      for (Map.Entry<String, String> userAuth : authentication.entrySet()) {
        // Check if the key is accepted by the provider
        if (!acceptedKeys.contains(userAuth.getKey())) {
          throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
              "Reranking provider '%s' does not support authentication key '%s'",
              provider, userAuth.getKey());
        }
      }
    }
    return authentication;
  }
}
