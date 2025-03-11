package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.constants.RerankingConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.util.HashMap;
import java.util.Map;

/** Validated configuration Object for Reranking configuration for Collections. */
public record CollectionRerankingConfig(
    boolean enabled,
    @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty("service")
        RerankProviderConfig rerankProviderConfig) {

  private static final RerankProviderConfig DEFAULT_RERANK_SERVICE =
      new RerankProviderConfig("nvidia", "nvidia/llama-3.2-nv-rerankqa-1b-v2", null, null);

  // Create a nested record for the provider-related fields
  public record RerankProviderConfig(
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
        enabled ? new RerankProviderConfig(provider, modelName, authentication, parameters) : null);
  }

  /**
   * Accessor for an instance to use for a default configuration for newly created collections:
   * where no configuration defined: needs to be enabled, using "nvidia" reranking service
   * configuration.
   */
  public static CollectionRerankingConfig configForNewCollections() {
    return new CollectionRerankingConfig(true, DEFAULT_RERANK_SERVICE);
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
    // TODO(Hazel): WHAT HAPPENS IF THEY DONT ? JSON props on VectorizeConfig say model is not
    // required
    String provider =
        rerankingServiceNode.get(RerankingConstants.RerankingService.PROVIDER).asText();
    String modelName =
        rerankingServiceNode.get(RerankingConstants.RerankingService.MODEL_NAME).asText();

    // construct VectorizeDefinition.authentication, can be null
    JsonNode authNode =
        rerankingServiceNode.get(RerankingConstants.RerankingService.AUTHENTICATION);
    // TODO(Hazel): remove unchecked assignment
    Map<String, String> authMap =
        authNode == null ? null : objectMapper.convertValue(authNode, Map.class);

    // construct VectorizeDefinition.parameters, can be null
    JsonNode paramsNode = rerankingServiceNode.get(RerankingConstants.RerankingService.PARAMETERS);
    // TODO(Hazel): remove unchecked assignment
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
      CreateCollectionCommand.Options.RerankingConfigDefinition rerankingConfig) {
    // If not defined, use default for new collections; valid option
    if (rerankingConfig == null) {
      return configForNewCollections();
    }
    // Otherwise validate and construct
    Boolean enabled = rerankingConfig.enabled();
    if (enabled == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'enabled' is required property for 'reranking' Object value");
    }

    var rerankingServiceConfig = rerankingConfig.rerankingServiceConfig();
    if (rerankingServiceConfig == null) {
      return new CollectionRerankingConfig(enabled, DEFAULT_RERANK_SERVICE);
    }

    String provider = rerankingConfig.rerankingServiceConfig().provider();
    String modelName = rerankingConfig.rerankingServiceConfig().modelName();
    Map<String, String> authentication = rerankingConfig.rerankingServiceConfig().authentication();
    Map<String, Object> parameters = rerankingConfig.rerankingServiceConfig().parameters();

    if (provider == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'provider' is required property for 'reranking.service' Object value");
    }

    if (authentication != null && !authentication.isEmpty()) {
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
    }

    return new CollectionRerankingConfig(enabled, provider, modelName, authentication, parameters);
  }

  // TODO(Hazel): need config verification for reranking service - like VectorizeConfigValidator
}
