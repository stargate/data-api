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
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ModelSupport;
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
  /**
   * Singleton instance for disabled reranking configuration. It can be used for disabled reranking
   * collections, existing pre-reranking collections, and missing collections.
   */
  public static final CollectionRerankDef DISABLED = new CollectionRerankDef(false, null);

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionRerankDef.class);

  private final boolean enabled;
  private final RerankServiceDef rerankServiceDef;

  /**
   * Constructs a reranking configuration with the specified enabled state and service
   * configuration.
   *
   * <p>This constructor is annotated with {@link JsonCreator} to enable Jackson deserialization.
   * {@link JsonProperty} annotations on parameters allow Jackson to map JSON properties to
   * constructor parameters during deserialization. This constructor is private - please use the
   * appropriate factory method to create instances.
   *
   * <p>Validation behavior:
   *
   * <ul>
   *   <li>If reranking is enabled (enabled = true), the service definition must not be null
   *   <li>If reranking is disabled (enabled = false), the service definition must be null
   * </ul>
   *
   * @param enabled Whether reranking is enabled for this collection
   * @param rerankServiceDef The service configuration for reranking, must not be null if reranking
   *     is enabled and must be null if reranking is disabled
   * @throws NullPointerException if reranking is enabled and rerankServiceDef is null
   * @throws IllegalArgumentException if reranking is disabled and rerankServiceDef is not null
   */
  @JsonCreator
  public CollectionRerankDef(
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("service") RerankServiceDef rerankServiceDef) {
    this.enabled = enabled;
    if (enabled) {
      this.rerankServiceDef =
          Objects.requireNonNull(
              rerankServiceDef,
              "Rerank service configuration must not be null when reranking is enabled");
    } else {
      if (rerankServiceDef != null) {
        throw new IllegalArgumentException(
            "Rerank service configuration must be null when reranking is disabled");
      }
      this.rerankServiceDef = null;
    }
  }

  /** Returns whether reranking is enabled for this collection. */
  @JsonProperty
  public boolean enabled() {
    return enabled;
  }

  /** Returns the reranking service configuration for this collection. */
  @JsonProperty("service")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public RerankServiceDef rerankServiceDef() {
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
   * @param isRerankingEnabledForAPI
   * @param rerankingProvidersConfig The configuration for all available reranking providers
   * @return A default-configured CollectionRerankDef
   */
  public static CollectionRerankDef configForNewCollections(
      boolean isRerankingEnabledForAPI, RerankingProvidersConfig rerankingProvidersConfig) {
    Objects.requireNonNull(rerankingProvidersConfig, "Reranking providers config cannot be null");
    // If reranking is not enabled for the API, return disabled configuration
    if (!isRerankingEnabledForAPI) {
      return DISABLED;
    }
    // Find the provider marked as default
    var defaultProviderEntry =
        rerankingProvidersConfig.providers().entrySet().stream()
            .filter(entry -> entry.getValue().isDefault())
            .findFirst();
    // If no default provider exists, disable reranking
    if (defaultProviderEntry.isEmpty()) {
      LOGGER.debug("No default reranking provider found, disabling reranking for new collections");
      return DISABLED;
    }

    // Extract provider information
    String defaultProviderName = defaultProviderEntry.get().getKey();
    var defaultProviderConfig = defaultProviderEntry.get().getValue();

    // Find the model marked as default for this provider
    // The default provider must have a default model that has SUPPORTED status, otherwise it's
    // config bug
    var defaultModel =
        defaultProviderConfig.models().stream()
            .filter(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig::isDefault)
            .filter(
                modelConfig ->
                    modelConfig.modelSupport().status() == ModelSupport.SupportStatus.SUPPORTED)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Default reranking provider '%s' does not have a default supported model"
                            .formatted(defaultProviderName)));

    // Check if the default provider supports the 'NONE' authentication type
    // If not, it's a config bug
    if (!defaultProviderConfig
        .supportedAuthentications()
        .containsKey(RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType.NONE)) {
      throw new IllegalStateException(
          "Default reranking provider '%s' does not support 'NONE' authentication type"
              .formatted(defaultProviderName));
    }

    // TODO(Hazel): Check if there is any parameter for the default model and if there is default
    // value for it
    // No parameters in the configuration now, leave it to the next PR

    // Create reranking service config with default provider and model
    // Authentication and parameters are intentionally null for default configs
    var defaultRerankingService =
        new RerankServiceDef(
            defaultProviderName,
            defaultModel.name(),
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
  public static CollectionRerankDef configForPreRerankingCollection() {
    return DISABLED;
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
      boolean isRerankingEnabledForAPI,
      CreateCollectionCommand.Options.RerankDesc rerankingDesc,
      RerankingProvidersConfig providerConfigs) {

    // If reranking is not enabled for the API, error out if user provides desc or return disabled
    // configuration.
    if (!isRerankingEnabledForAPI) {
      if (rerankingDesc != null) {
        throw ErrorCodeV1.RERANKING_FEATURE_NOT_ENABLED.toApiException();
      }
      return DISABLED;
    }

    // Case 1: No configuration provided - use defaults
    if (rerankingDesc == null) {
      return configForNewCollections(isRerankingEnabledForAPI, providerConfigs);
    }

    // Case 2: Validate 'enabled' flag is present
    Boolean enabled = rerankingDesc.enabled();
    var serviceConfig = rerankingDesc.rerankServiceDesc();
    if (enabled == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'enabled' is required property for 'rerank' Object value");
    }

    // Case 3: Reranking disabled - ensure no service configuration is provided
    if (!enabled) {
      if (serviceConfig != null && !serviceConfig.isEmpty()) {
        throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
            "'rerank' is disabled, but 'rerank.service' configuration is provided");
      }
      return DISABLED;
    }

    // Case 4: Enabled but no service config - use defaults
    if (serviceConfig == null) {
      return configForNewCollections(isRerankingEnabledForAPI, providerConfigs);
    }

    // Case 5: Full configuration - validate all components
    var provider = rerankingDesc.rerankServiceDesc().provider();
    var providerConfig = getAndValidateProviderConfig(provider, providerConfigs);

    // Create validated configuration
    return new CollectionRerankDef(
        enabled,
        new RerankServiceDef(
            provider,
            validateModel(provider, serviceConfig.modelName(), providerConfig),
            validateAuthentication(provider, serviceConfig.authentication(), providerConfig),
            validateParameters(provider, serviceConfig.parameters(), providerConfig)));
  }

  /**
   * Converts this internal reranking representation to the external API representation. This method
   * is used in {@link CollectionSchemaObject} and FindCollection command, it converts collection
   * comments -> CollectionSchemaObject -> CreateCollectionCommand
   */
  public CreateCollectionCommand.Options.RerankDesc toRerankDesc() {
    if (!enabled) {
      return new CreateCollectionCommand.Options.RerankDesc(false, null);
    }
    if (rerankServiceDef == null) {
      throw new IllegalStateException(
          "Collection rerankServiceDef should not be null while rerank is enabled");
    }
    CreateCollectionCommand.Options.RerankServiceDesc rerankServiceDesc =
        new CreateCollectionCommand.Options.RerankServiceDesc(
            rerankServiceDef.provider(),
            rerankServiceDef.modelName(),
            rerankServiceDef.authentication(),
            rerankServiceDef.parameters());

    return new CreateCollectionCommand.Options.RerankDesc(true, rerankServiceDesc);
  }

  /**
   * Validates and retrieves the configuration for a reranking provider.
   *
   * @param provider The reranking provider name.
   * @param rerankingProvidersConfig The configuration containing all available reranking providers.
   * @return The validated provider configuration.
   * @throws JsonApiException If the provider is null, not found, or not enabled.
   */
  private static RerankingProvidersConfig.RerankingProviderConfig getAndValidateProviderConfig(
      String provider, RerankingProvidersConfig rerankingProvidersConfig) {
    // 1. Ensures the provider name is specified (not null)
    if (provider == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Provider name is required for reranking service configuration");
    }

    // 2. Verifies the provider exists in configuration and is enabled (includes null and empty
    // check)
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

    var rerankModel =
        rerankingProviderConfig.models().stream()
            .filter(modelConfig -> modelConfig.name().equals(modelName))
            .findFirst();

    if (rerankModel.isEmpty()) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Model '%s' is not supported by reranking provider '%s'", modelName, provider);
    }

    var model = rerankModel.get();
    if (model.modelSupport().status() != ModelSupport.SupportStatus.SUPPORTED) {
      throw SchemaException.Code.UNSUPPORTED_PROVIDER_MODEL.get(
          Map.of(
              "model",
              model.name(),
              "modelStatus",
              model.modelSupport().status().name(),
              "message",
              model.modelSupport().message().orElse("The model is not supported.")));
    }

    return modelName;
  }

  /**
   * Validates authentication parameters for reranking models when creating a collection. Currently,
   * this method enforces that no authentication details are provided, as all supported reranking
   * providers use the 'NONE' or 'HEADER' authentication type. Add more verifications if more
   * authentication types are supported in the future.
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
    // Currently, all supported reranking providers use the 'NONE' or 'HEADER' authentication type,
    // so the authentication map must be null or empty
    if (authentication != null && !authentication.isEmpty()) {
      // Check if the provider supports 'NONE' or 'HEADER' authentication
      Map<RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType, ?> supportedAuth =
          rerankingProviderConfig.supportedAuthentications();

      if (supportedAuth.containsKey(
              RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType.NONE)
          || supportedAuth.containsKey(
              RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType.HEADER)) {
        throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
            "Reranking provider '%s' currently only supports 'NONE' or 'HEADER' authentication types. No authentication parameters should be provided.",
            provider);
      }
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
