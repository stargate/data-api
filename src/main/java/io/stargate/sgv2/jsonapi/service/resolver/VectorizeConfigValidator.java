package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.provider.ModelSupport;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class that has validation for vectorize configuration. It is used by both collection and tables
 * api.
 */
@ApplicationScoped
public class VectorizeConfigValidator {
  private final OperationsConfig operationsConfig;
  private final EmbeddingProvidersConfig embeddingProvidersConfig;
  private final ValidateCredentials validateCredentials;

  @Inject
  public VectorizeConfigValidator(
      OperationsConfig operationsConfig,
      EmbeddingProvidersConfig embeddingProvidersConfig,
      ValidateCredentials validateCredentials) {
    this.operationsConfig = operationsConfig;
    this.embeddingProvidersConfig = embeddingProvidersConfig;
    this.validateCredentials = validateCredentials;
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
  public Integer validateService(VectorizeConfig userConfig, Integer userVectorDimension) {
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
    // huggingFaceDedicated: must have vectorDimension specified
    // other providers: must have model specified, and default dimension when dimension not
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
      VectorizeConfig userConfig) {
    EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig =
        embeddingProvidersConfig.providers().get(userConfig.provider());
    if (providerConfig == null || !providerConfig.enabled()) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Service provider '%s' is not supported", userConfig.provider());
    }
    return providerConfig;
  }

  /**
   * Validates user authentication for creating a collection using the specified configurations.
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
   * @param userConfig The vectorize configuration provided by the user.
   * @param providerConfig The embedding provider configuration.
   * @throws JsonApiException If the user authentication is invalid.
   */
  private void validateAuthentication(
      VectorizeConfig userConfig, EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {
    // Get all the accepted keys in SHARED_SECRET
    Set<String> acceptedKeys =
        providerConfig.supportedAuthentications().entrySet().stream()
            .filter(
                config ->
                    config
                        .getKey()
                        .equals(
                            EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType
                                .SHARED_SECRET))
            .filter(config -> config.getValue().enabled() && config.getValue().tokens() != null)
            .flatMap(config -> config.getValue().tokens().stream())
            .map(EmbeddingProvidersConfig.EmbeddingProviderConfig.TokenConfig::accepted)
            .collect(Collectors.toSet());

    // If the user hasn't provided authentication details, verify that either the 'NONE' or 'HEADER'
    // authentication type is enabled.
    if (userConfig.authentication() == null || userConfig.authentication().isEmpty()) {
      // Check if 'NONE' authentication type is enabled
      boolean noneEnabled =
          Optional.ofNullable(
                  providerConfig
                      .supportedAuthentications()
                      .get(
                          EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType.NONE))
              .map(EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig::enabled)
              .orElse(false);

      // Check if 'HEADER' authentication type is enabled
      boolean headerEnabled =
          Optional.ofNullable(
                  providerConfig
                      .supportedAuthentications()
                      .get(
                          EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType
                              .HEADER))
              .map(EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig::enabled)
              .orElse(false);

      // If neither 'NONE' nor 'HEADER' authentication type is enabled, throw an exception
      if (!noneEnabled && !headerEnabled) {
        throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
            "Service provider '%s' does not support either 'NONE' or 'HEADER' authentication types.",
            userConfig.provider());
      }
    } else {
      // User has provided authentication details. Validate each key against the provider's accepted
      // list.
      for (Map.Entry<String, String> userAuth : userConfig.authentication().entrySet()) {
        // Check if the key is accepted by the provider
        if (!acceptedKeys.contains(userAuth.getKey())) {
          throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
              "Service provider '%s' does not support authentication key '%s'",
              userConfig.provider(), userAuth.getKey());
        }

        // Validate the credential name from secret service
        // already append the .providerKey to the value in CreateCollectionCommand
        if (operationsConfig.enableEmbeddingGateway()) {
          validateCredentials.validate(userConfig.provider(), userAuth.getValue());
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
      VectorizeConfig userConfig, EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {
    // 0. Combine provider level and model level parameters
    List<EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig> allParameters =
        new ArrayList<>();
    // Add all provider level parameters
    allParameters.addAll(providerConfig.parameters());
    // Get all the parameters except "vectorDimension" for the model -- model has been validated in
    // the previous step, huggingfaceDedicated uses endpoint-defined-model
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
                throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
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
            throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
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
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "The provided parameter '%s' type is incorrect. Expected: '%s'",
          expectedParamConfig.name(), expectedParamType);
    }
  }

  /**
   * Validate the model support first. see {@link ModelSupport}
   *
   * <ul>
   *   <li>SUPPORTED: validation will pass
   *   <li>DEPRECATED: can not create new collection/table>, prompt support message
   *   <li>END_OF_LIFE: can not create new collection/table>, prompt support message
   * </ul>
   *
   * Then Validates the model name and vector dimension provided in the user configuration against
   * the specified embedding provider configuration.
   *
   * @param userConfig the user-specified vectorization configuration
   * @param providerConfig the configuration of the embedding provider
   * @param userVectorDimension the vector dimension provided by the user, or null if not provided
   * @return the validated vector dimension to be used for the model
   * @throws JsonApiException if the model name is not found, or if the dimension is invalid
   */
  private Integer validateModelAndDimension(
      VectorizeConfig userConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      Integer userVectorDimension) {

    // Find the model configuration by matching the model name
    // 1. huggingfaceDedicated does not require model, but requires dimension
    if (userConfig.provider().equals(ProviderConstants.HUGGINGFACE_DEDICATED)) {
      if (userVectorDimension == null) {
        throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
            "'dimension' is needed for provider %s", ProviderConstants.HUGGINGFACE_DEDICATED);
      }
    }

    // 2. other providers do require model
    if (userConfig.modelName() == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'modelName' is needed for provider %s", userConfig.provider());
    }
    EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model =
        providerConfig.models().stream()
            .filter(m -> m.name().equals(userConfig.modelName()))
            .findFirst()
            .orElseThrow(
                () ->
                    ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
                        "Model name '%s' for provider '%s' is not supported",
                        userConfig.modelName(), userConfig.provider()));

    // validate model support
    if (model.modelSupport().status() != ModelSupport.SupportStatus.SUPPORTED) {
      throw SchemaException.Code.UNSUPPORTED_PROVIDER_MODEL.get(
          Map.of(
              "model",
              userConfig.modelName(),
              "modelStatus",
              model.modelSupport().status().name(),
              "message",
              model.modelSupport().message().orElse("The model is not supported.")));
    }

    // Handle models with a fixed vector dimension
    if (model.vectorDimension().isPresent() && model.vectorDimension().get() != 0) {
      Integer configVectorDimension = model.vectorDimension().get();
      if (userVectorDimension == null) {
        return configVectorDimension; // Use model's dimension if user hasn't specified any
      } else if (!configVectorDimension.equals(userVectorDimension)) {
        throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
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
          throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
              "The provided dimension value (%d) is not within the supported numeric range [%d, %d]",
              userVectorDimension, validationValues.get(0), validationValues.get(1));
        }
      }
      case OPTIONS -> {
        if (!validationValues.contains(userVectorDimension)) {
          String validatedValuesStr =
              String.join(
                  ", ", validationValues.stream().map(Object::toString).toArray(String[]::new));
          throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
              "The provided dimension value '%s' is not within the supported options [%s]",
              userVectorDimension, validatedValuesStr);
        }
      }
    }
    return userVectorDimension;
  }
}
