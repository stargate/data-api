package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProviderConfigProducer;
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
   * collections and missing collections.
   */
  private static final CollectionRerankDef DISABLED = new CollectionRerankDef(false, null);

  /**
   * Singleton instance for disabled reranking configuration. It is to be used for existing
   * pre-reranking collections.
   */
  private static final CollectionRerankDef MISSING = new CollectionRerankDef(false, null);

  /**
   * Singleton instance for default reranking configuration. It is used for newly created
   * collections with default reranking settings.
   *
   * <p>NOTE: this is initialized during startup (via call to {@link #initializeDefaultRerankDef} by
   * {@link RerankingProviderConfigProducer}) and cannot unfortunately be made final: this because
   * initialization requires access to other configuration loaded during start up.
   */
  private static CollectionRerankDef DEFAULT;

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
   * Get default reranking configuration for new collections.
   *
   * <p>When a collection is created without explicit reranking settings, this method provides a
   * default configuration based on the reranking providers' configuration. It looks for the
   * provider marked as default and its default model.
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
    if (DEFAULT == null) {
      // DEFAULT has been set during the application startup.
      throw new IllegalStateException("No default reranking definition found");
    }
    return DEFAULT;
  }

  /**
   * Initializes the DEFAULT reranking definition as Singleton during the application startup. See
   * {@link RerankingProviderConfigProducer} as caller and how the configuration is validated to
   * promise a default provider and model.
   */
  public static void initializeDefaultRerankDef(RerankingProvidersConfig rerankingProvidersConfig) {
    // Find the provider marked as default
    var defaultProviderEntry =
        rerankingProvidersConfig.providers().entrySet().stream()
            .filter(entry -> entry.getValue().isDefault())
            .findFirst();
    // There must be a default provider, otherwise it's a config bug.
    // It is validated in RerankingProviderConfigProducer.class during startup.
    if (defaultProviderEntry.isEmpty()) {
      throw new IllegalStateException("No default reranking provider found");
    }

    // Extract provider information
    String defaultProviderName = defaultProviderEntry.get().getKey();
    var defaultProviderConfig = defaultProviderEntry.get().getValue();

    // Find the model marked as default for this provider.
    // The default provider must have a default model that has SUPPORTED status, otherwise it's
    // config bug, It is validated in RerankingProviderConfigProducer.class during startup.
    var defaultModel =
        defaultProviderConfig.models().stream()
            .filter(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig::isDefault)
            .filter(
                modelConfig ->
                    modelConfig.apiModelSupport().status()
                        == ApiModelSupport.SupportStatus.SUPPORTED)
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

    LOGGER.info(
        "InitializeDefaultRerankDef during application startup, default reranking configuration initialized with provider '%s' and model '%s'"
            .formatted(defaultProviderName, defaultModel.name()));
    DEFAULT = new CollectionRerankDef(true, defaultRerankingService);
  }

  public static CollectionRerankDef configForDisabled() {
    return DISABLED;
  }

  /**
   * Accessor for getting a configuration for existing collections that predate reranking support.
   *
   * <p>Used for collections created before reranking functionality was available. These collections
   * need to have reranking explicitly disabled for backward compatibility.
   *
   * @return A singleton CollectionRerankDef instance ({@link #MISSING}) with reranking disabled
   */
  public static CollectionRerankDef configForPreRerankingCollection() {
    return MISSING;
  }

  /**
   * Accessor for a singleton instance used to represent case of default reranking configuration for
   * newly created Collections that do not specify reranking configuration.
   *
   * @return A singleton CollectionRerankDef instance ({@link #DEFAULT}) initialized during
   *     application startup.
   */
  public static CollectionRerankDef configForDefault() {
    return DEFAULT;
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
   * @throws APIException if the configuration is invalid
   */
  public static CollectionRerankDef fromApiDesc(
      boolean isRerankingEnabledForAPI,
      CreateCollectionCommand.Options.RerankDesc rerankingDesc,
      RerankingProvidersConfig providerConfigs) {

    // If reranking is not enabled for the API, allow explicit "enabled: false" but error out
    // if user tries to enable it (fix for #2423).
    if (!isRerankingEnabledForAPI) {
      if (rerankingDesc != null && !Boolean.FALSE.equals(rerankingDesc.enabled())) {
        throw SchemaException.Code.RERANKING_FEATURE_NOT_ENABLED.get();
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
      throw SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS.get(
          "message", "'enabled' is required property for 'rerank' Object value");
    }

    // Case 3: Reranking disabled - ensure no service configuration is provided
    if (!enabled) {
      if (serviceConfig != null && !serviceConfig.isEmpty()) {
        throw SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS.get(
            "message", "'rerank' is disabled, but 'rerank.service' configuration is provided");
      }
      return DISABLED;
    }

    // Case 4: Enabled but no service config - use defaults
    if (serviceConfig == null) {
      return configForNewCollections(isRerankingEnabledForAPI, providerConfigs);
    }

    // Case 5: Full configuration - validate all components
    return new CollectionRerankDef(
        enabled,
        validateServiceDesc(
            providerConfigs,
            serviceConfig.provider(),
            serviceConfig.modelName(),
            serviceConfig.authentication(),
            serviceConfig.parameters(),
            SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS));
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
   * Validates provider, model, authentication, and parameters against the reranking provider
   * configuration and returns a validated {@link RerankServiceDef}. Reused by both {@link
   * #fromApiDesc} (collection creation) and the {@code findAndRerank} command override path.
   *
   * @param providerConfigs The reranking provider configuration.
   * @param provider The provider name.
   * @param modelName The model name.
   * @param authentication Authentication configuration (may be null).
   * @param parameters Additional parameters (may be null).
   * @param validationErrorCode The error code to use for validation failures (provider/model not
   *     found, disabled, etc.). Model support status errors always use {@link
   *     SchemaException.Code#DEPRECATED_AI_MODEL} or {@link
   *     SchemaException.Code#END_OF_LIFE_AI_MODEL}.
   * @return A validated RerankServiceDef.
   */
  public static RerankServiceDef validateServiceDesc(
      RerankingProvidersConfig providerConfigs,
      String provider,
      String modelName,
      Map<String, String> authentication,
      Map<String, Object> parameters,
      ErrorCode<? extends APIException> validationErrorCode) {

    var providerConfig =
        getAndValidateProviderConfig(provider, providerConfigs, validationErrorCode);
    return new RerankServiceDef(
        provider,
        validateModel(provider, modelName, providerConfig, validationErrorCode),
        validateAuthentication(provider, authentication, providerConfig, validationErrorCode),
        validateParameters(provider, parameters, providerConfig, validationErrorCode));
  }

  /**
   * Checks the model support status for an existing {@link RerankServiceDef}. Used at query time
   * when a collection's configured model may have reached end-of-life after collection creation.
   * Only blocks {@link ApiModelSupport.SupportStatus#END_OF_LIFE END_OF_LIFE}; deprecated models
   * are still allowed for existing collections.
   *
   * @param providerConfigs The reranking provider configuration.
   * @param serviceDef The service definition to check.
   */
  public static void checkExistingModelStatus(
      RerankingProvidersConfig providerConfigs, RerankServiceDef serviceDef) {
    var modelConfig = providerConfigs.filterByRerankServiceDef(serviceDef);
    if (modelConfig.apiModelSupport().status() == ApiModelSupport.SupportStatus.END_OF_LIFE) {
      throw SchemaException.Code.END_OF_LIFE_AI_MODEL.get(
          Map.of(
              "model",
              modelConfig.name(),
              "modelStatus",
              modelConfig.apiModelSupport().status().name(),
              "message",
              modelConfig
                  .apiModelSupport()
                  .message()
                  .orElse("The model is no longer supported (reached its end-of-life).")));
    }
  }

  private static RerankingProvidersConfig.RerankingProviderConfig getAndValidateProviderConfig(
      String provider,
      RerankingProvidersConfig rerankingProvidersConfig,
      ErrorCode<? extends APIException> errorCode) {
    if (provider == null) {
      throw errorCode.get(
          "message", "Provider name is required for reranking service configuration");
    }

    var providerConfig = rerankingProvidersConfig.providers().get(provider);
    if (providerConfig == null) {
      throw errorCode.get(
          "message", "Reranking provider '%s' is not supported".formatted(provider));
    }

    if (!providerConfig.enabled()) {
      throw errorCode.get("message", "Reranking provider '%s' is disabled".formatted(provider));
    }
    return providerConfig;
  }

  private static String validateModel(
      String provider,
      String modelName,
      RerankingProvidersConfig.RerankingProviderConfig rerankingProviderConfig,
      ErrorCode<? extends APIException> errorCode) {
    if (modelName == null) {
      throw errorCode.get(
          "message", "Model name is required for reranking provider '%s'".formatted(provider));
    }

    var rerankModel =
        rerankingProviderConfig.models().stream()
            .filter(modelConfig -> modelConfig.name().equals(modelName))
            .findFirst();

    if (rerankModel.isEmpty()) {
      throw errorCode.get(
          "message",
          "Model '%s' is not supported by reranking provider '%s'".formatted(modelName, provider));
    }

    // Model support status errors always use DEPRECATED_AI_MODEL / END_OF_LIFE_AI_MODEL
    // regardless of the caller's validationErrorCode.
    var model = rerankModel.get();
    if (model.apiModelSupport().status() != ApiModelSupport.SupportStatus.SUPPORTED) {
      var statusErrorCode =
          model.apiModelSupport().status() == ApiModelSupport.SupportStatus.DEPRECATED
              ? SchemaException.Code.DEPRECATED_AI_MODEL
              : SchemaException.Code.END_OF_LIFE_AI_MODEL;
      throw statusErrorCode.get(
          Map.of(
              "model",
              model.name(),
              "modelStatus",
              model.apiModelSupport().status().name(),
              "message",
              model
                  .apiModelSupport()
                  .message()
                  .orElse("The model is %s.".formatted(model.apiModelSupport().status().name()))));
    }

    return modelName;
  }

  private static Map<String, String> validateAuthentication(
      String provider,
      Map<String, String> authentication,
      RerankingProvidersConfig.RerankingProviderConfig rerankingProviderConfig,
      ErrorCode<? extends APIException> errorCode) {
    if (authentication != null && !authentication.isEmpty()) {
      Map<RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType, ?> supportedAuth =
          rerankingProviderConfig.supportedAuthentications();

      if (supportedAuth.containsKey(
              RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType.NONE)
          || supportedAuth.containsKey(
              RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType.HEADER)) {
        throw errorCode.get(
            "message",
            "Reranking provider '%s' currently only supports 'NONE' or 'HEADER' authentication types. No authentication parameters should be provided."
                .formatted(provider));
      }
    }
    return authentication;
  }

  private static Map<String, Object> validateParameters(
      String provider,
      Map<String, Object> parameters,
      RerankingProvidersConfig.RerankingProviderConfig rerankingProviderConfig,
      ErrorCode<? extends APIException> errorCode) {
    if (parameters != null && !parameters.isEmpty()) {
      throw errorCode.get(
          "message",
          "Reranking provider '%s' currently doesn't support any parameters. No parameters should be provided."
              .formatted(provider));
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
