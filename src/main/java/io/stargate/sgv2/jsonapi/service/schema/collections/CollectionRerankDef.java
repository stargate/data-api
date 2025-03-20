package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal configuration class for managing reranking settings for collections.
 *
 * <p>This class serves three main purposes in the collection lifecycle:
 *
 * <ol>
 *   <li>During collection creation: Validates and transforms user-provided configuration
 *       (deserialized from API as {@link CreateCollectionCommand.Options.RerankDesc}) into a
 *       validated internal representation via {@link #fromApiDesc}.
 *   <li>For persistence: After validation, instances of this class are serialized to JSON and
 *       stored in the database as part of the collection comment.
 *   <li>During query operations: Deserializes stored configuration in the comment via {@link
 *       #fromCommentJson} to be used during reranking operations.
 * </ol>
 *
 * <p>Note: This class is for internal use only and is not directly exposed through the DataAPI. The
 * DataAPI exposes {@link CreateCollectionCommand.Options.RerankDesc} for user input, which is then
 * validated and transformed into instances of this class before being persisted in the collection's
 * metadata comment.
 */
public class CollectionRerankDef {
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionRerankDef.class);
  private final boolean enabled;
  private final RerankServiceDef rerankServiceDef;

  /**
   * Singleton instance for disabled reranking configuration. It can be used for disabled reranking
   * collections, existing pre-reranking collections, and missing collections.
   */
  private static final CollectionRerankDef DISABLED_RERANKING_CONFIG =
      new CollectionRerankDef(false, null);

  /**
   * Constructs a reranking configuration with the specified enabled state and service
   * configuration.
   *
   * @param enabled Whether reranking is enabled for this collection
   * @param rerankServiceDef The service configuration for reranking, can be null if reranking is
   *     disabled
   */
  @JsonCreator
  public CollectionRerankDef(
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("service") RerankServiceDef rerankServiceDef) {
    this.enabled = enabled;
    this.rerankServiceDef = rerankServiceDef;
  }

  /**
   * Constructs a detailed reranking configuration with individual service parameters.
   *
   * @param enabled Whether reranking is enabled for this collection
   * @param provider The name of the reranking provider
   * @param modelName The name of the reranking model
   * @param authentication Authentication parameters for the reranking service
   * @param parameters Additional parameters for the reranking service
   */
  public CollectionRerankDef(
      boolean enabled,
      String provider,
      String modelName,
      Map<String, String> authentication,
      Map<String, Object> parameters) {
    // Clear out any reranking settings if not enabled (but don't fail)
    this(
        enabled,
        enabled ? new RerankServiceDef(provider, modelName, authentication, parameters) : null);
  }

  /** Returns whether reranking is enabled for this collection. */
  @JsonProperty
  public boolean enabled() {
    return enabled;
  }

  /** Returns the reranking service configuration for this collection. */
  @JsonProperty("service")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public RerankServiceDef rerankingProviderConfig() {
    return rerankServiceDef;
  }

  /**
   * Creates default reranking configuration for new collections.
   *
   * <p>When a collection is created without explicit reranking settings, this method provides a
   * default configuration based on the reranking providers' configuration. It looks for the
   * provider marked as default and its default model.
   *
   * <p>If no default provider is configured in the yaml, reranking will be disabled for new
   * collections. Similarly, if the default provider doesn't have a default model, reranking will be
   * disabled.
   *
   * @param rerankingProvidersConfig The configuration for all available reranking providers
   * @return A default-configured CollectionRerankDef
   */
  public static CollectionRerankDef configForNewCollections(
      RerankingProvidersConfig rerankingProvidersConfig) {
    Objects.requireNonNull(rerankingProvidersConfig, "Reranking providers config cannot be null");
    // Find the provider marked as default
    var defaultProviderEntry =
        rerankingProvidersConfig.providers().entrySet().stream()
            .filter(entry -> entry.getValue().isDefault())
            .findFirst();
    // If no default provider exists, disable reranking
    if (defaultProviderEntry.isEmpty()) {
      return DISABLED_RERANKING_CONFIG;
    }

    // Extract provider information
    String providerName = defaultProviderEntry.get().getKey();
    RerankingProvidersConfig.RerankingProviderConfig providerConfig =
        defaultProviderEntry.get().getValue();

    // Find the model marked as default for this provider
    Optional<String> defaultModelName =
        providerConfig.models().stream()
            .filter(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig::isDefault)
            .map(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig::name)
            .findFirst();

    // If no default model exists, disable reranking
    if (defaultModelName.isEmpty()) {
      return DISABLED_RERANKING_CONFIG;
    }

    // Create reranking service config with default provider and model
    // Authentication and parameters are intentionally null for default configs
    var defaultRerankingService =
        new RerankServiceDef(
            providerName,
            defaultModelName.get(),
            null, // No authentication for default configuration
            null // No parameters for default configuration
            );

    return new CollectionRerankDef(true, defaultRerankingService);
  }

  /**
   * Factory method for creating a configuration for existing collections that predate reranking
   * support.
   *
   * <p>Used for collections created before reranking functionality was available. These collections
   * need to have reranking explicitly disabled for backward compatibility.
   *
   * @return A singleton CollectionRerankDef instance with reranking disabled
   */
  public static CollectionRerankDef configForPreRerankingCollections() {
    return DISABLED_RERANKING_CONFIG;
  }

  /**
   * Factory method for creating a configuration when a collection definition is missing.
   *
   * @return A singleton CollectionRerankDef instance with reranking disabled
   */
  public static CollectionRerankDef configForMissingCollection() {
    return DISABLED_RERANKING_CONFIG;
  }

  /**
   * This method is used when retrieving the stored reranking configuration from the collection
   * comments during query operations. It transforms the stored JSON representation back into a
   * usable configuration object using Jackson's automatic deserialization capabilities.
   *
   * @param keyspaceName The name of the keyspace - used for logging and error messages
   * @param collectionName The name of the collection - used for logging and error messages
   * @param rerankingJsonNode The JSON node containing the stored reranking configuration
   * @param objectMapper The object mapper to use for JSON conversion
   * @return A CollectionRerankDef object reconstructed from the stored JSON
   * @throws IllegalArgumentException if the JSON cannot be properly deserialized
   */
  public static CollectionRerankDef fromCommentJson(
      String keyspaceName,
      String collectionName,
      JsonNode rerankingJsonNode,
      ObjectMapper objectMapper) {
    try {
      return objectMapper.treeToValue(rerankingJsonNode, CollectionRerankDef.class);
    } catch (JsonProcessingException | IllegalArgumentException e) {
      LOGGER.error(
          "Error parsing reranking JSON configuration from the collection '%s.%s' comment, json: %s"
              .formatted(keyspaceName, collectionName, rerankingJsonNode.toString()),
          e);
      throw new IllegalArgumentException(
          "Invalid reranking configuration for collection '%s.%s'"
              .formatted(keyspaceName, collectionName));
    }
  }

  /**
   * This method is used during collection creation to validate and transform the user-provided
   * configuration from the DataAPI ({@link CreateCollectionCommand.Options.RerankDesc}) into an
   * internal configuration object. The resulting object will be persisted to the database.
   *
   * @param rerankingDesc The reranking configuration received from API input (may be null)
   * @param providerConfigs The reranking configuration yaml for available reranking providers
   * @return A validated CollectionRerankDef object
   * @throws JsonApiException if the configuration is invalid
   */
  public static CollectionRerankDef fromApiDesc(
      CreateCollectionCommand.Options.RerankDesc rerankingDesc,
      RerankingProvidersConfig providerConfigs) {
    // Case 1: No configuration provided - use defaults
    if (rerankingDesc == null) {
      return configForNewCollections(providerConfigs);
    }

    // Case 2: Validate 'enabled' flag is present
    Boolean enabled = rerankingDesc.enabled();
    if (enabled == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'enabled' is required property for 'rerank' Object value");
    }

    // Case 3: Reranking disabled - return simple disabled config
    if (!enabled) {
      return DISABLED_RERANKING_CONFIG;
    }

    // Case 4: Enabled but no service config - use defaults
    var serviceConfig = rerankingDesc.rerankServiceDesc();
    if (serviceConfig == null) {
      return configForNewCollections(providerConfigs);
    }

    // Case 5: Full configuration - validate all components
    // Extract values from API config
    String provider = rerankingDesc.rerankServiceDesc().provider();
    String modelName = rerankingDesc.rerankServiceDesc().modelName();
    Map<String, String> authentication = rerankingDesc.rerankServiceDesc().authentication();
    Map<String, Object> parameters = rerankingDesc.rerankServiceDesc().parameters();

    // Validate against the yaml  configuration
    var providerConfig = getAndValidateProviderConfig(provider, providerConfigs);
    modelName = validateModel(provider, modelName, providerConfig);
    authentication = validateAuthentication(provider, authentication, providerConfig);
    parameters = validateParameters(provider, parameters, providerConfig);

    // Create validated configuration
    return new CollectionRerankDef(enabled, provider, modelName, authentication, parameters);
  }

  /**
   * Validates and retrieves the configuration for a reranking provider. This method performs two
   * validations:<br>
   * 1. Ensures the provider name is specified (not null)<br>
   * 2. Verifies the provider exists in configuration and is enabled (includes null and empty check)
   *
   * @param provider The reranking provider name.
   * @param rerankingProvidersConfig The configuration containing all available reranking providers.
   * @return The validated provider configuration.
   * @throws JsonApiException If the provider is null, not found, or not enabled.
   */
  private static RerankingProvidersConfig.RerankingProviderConfig getAndValidateProviderConfig(
      String provider, RerankingProvidersConfig rerankingProvidersConfig) {
    if (provider == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Provider name is required for reranking service configuration");
    }

    var providerConfig = rerankingProvidersConfig.providers().get(provider);
    if (providerConfig == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Reranking provider '%s' is not supported", provider);
    }

    if (!providerConfig.enabled()) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Reranking provider '%s' is disabled", provider);
    }
    return providerConfig;
  }

  /**
   * Validates the model name for a reranking provider when creating a collection. This method
   * performs two validations:<br>
   * 1. Ensures the model name is provided (not null) <br>
   * 2. Verifies the model is supported by the specified provider according to configuration
   * (includes null and empty check)
   *
   * @param provider The reranking provider name.
   * @param modelName The name of the reranking model to validate.
   * @param rerankingProviderConfig The configuration for the specified reranking provider.
   * @return The validated model name if it passes all checks.
   * @throws JsonApiException If the model name is null or not supported by the provider.
   */
  private static String validateModel(
      String provider,
      String modelName,
      RerankingProvidersConfig.RerankingProviderConfig rerankingProviderConfig) {
    if (modelName == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Model name is required for reranking provider '%s'", provider);
    }

    boolean isModelSupported =
        rerankingProviderConfig.models().stream().anyMatch(m -> m.name().equals(modelName));

    if (!isModelSupported) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Model '%s' is not supported by reranking provider '%s'", modelName, provider);
    }
    return modelName;
  }

  /**
   * Validates authentication parameters for reranking models when creating a collection. Currently,
   * this method enforces that no authentication details are provided, as all supported reranking
   * providers use the 'NONE' authentication type. Add more verifications if more authentication
   * types are supported in the future.
   *
   * @param provider The reranking provider name.
   * @param authentication The reranking authentication details.
   * @param rerankingProviderConfig The configuration for the specified reranking provider.
   * @return The validated authentication map.
   * @throws JsonApiException If authentication parameters are provided when none are expected.
   */
  private static Map<String, String> validateAuthentication(
      String provider,
      Map<String, String> authentication,
      RerankingProvidersConfig.RerankingProviderConfig rerankingProviderConfig) {
    // Currently, all supported reranking providers use the 'NONE' authentication type,
    // so the authentication map must be null or empty
    if (authentication != null && !authentication.isEmpty()) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Reranking provider '%s' currently supports only the 'NONE' authentication type. No authentication parameters should be provided.",
          provider);
    }
    return authentication;
  }

  /**
   * Validates parameters for reranking models when creating a collection. Currently, this method
   * enforces that no parameters are provided, as all supported reranking providers don't accept any
   * parameters. Add more verifications if parameters are supported in the future.
   *
   * @param provider The reranking provider name.
   * @param parameters The reranking parameters (expected to be null or empty).
   * @param rerankingProviderConfig The configuration for the specified reranking provider.
   * @return The validated parameters map (null or empty for currently supported providers).
   * @throws JsonApiException If parameters are provided when none are expected.
   */
  private static Map<String, Object> validateParameters(
      String provider,
      Map<String, Object> parameters,
      RerankingProvidersConfig.RerankingProviderConfig rerankingProviderConfig) {
    // Currently, all supported reranking providers don't accept any parameters,
    // so the parameters map must be null or empty
    if (parameters != null && !parameters.isEmpty()) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Reranking provider '%s' currently doesn't support any parameters. No parameters should be provided.",
          provider);
    }
    return parameters;
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled, rerankServiceDef);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    CollectionRerankDef that = (CollectionRerankDef) obj;
    return enabled == that.enabled && Objects.equals(rerankServiceDef, that.rerankServiceDef);
  }

  @Override
  public String toString() {
    return "CollectionRerankDef["
        + "enabled="
        + enabled
        + ", "
        + "rerankServiceDesc="
        + rerankServiceDef
        + ']';
  }

  // Create a nested record for the provider-related fields
  public record RerankServiceDef(
      String provider,
      String modelName,
      Map<String, String> authentication,
      Map<String, Object> parameters) {}
}
