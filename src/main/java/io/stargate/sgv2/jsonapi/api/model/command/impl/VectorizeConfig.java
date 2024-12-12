package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.*;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

public record VectorizeConfig(
    @NotNull
        @Schema(
            description = "Registered Embedding service provider",
            type = SchemaType.STRING,
            implementation = String.class)
        @JsonProperty(VectorConstants.Vectorize.PROVIDER)
        String provider,
    @Schema(
            description = "Registered Embedding service model",
            type = SchemaType.STRING,
            implementation = String.class)
        @JsonProperty(VectorConstants.Vectorize.MODEL_NAME)
        String modelName,
    @Valid
        @Nullable
        @Schema(
            description = "Authentication config for chosen embedding service",
            type = SchemaType.OBJECT)
        @JsonProperty(VectorConstants.Vectorize.AUTHENTICATION)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        Map<String, String> authentication,
    @Nullable
        @Schema(
            description =
                "Optional parameters that match the messageTemplate provided for the provider",
            type = SchemaType.OBJECT)
        @JsonProperty(VectorConstants.Vectorize.PARAMETERS)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        Map<String, Object> parameters) {

  public VectorizeConfig(
      String provider,
      String modelName,
      Map<String, String> authentication,
      Map<String, Object> parameters) {
    if (provider == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException("'provider' is required");
    }
    this.provider = provider;
    // HuggingfaceDedicated does not need user to specify model explicitly
    // If user specifies modelName other than endpoint-defined-model, will error out
    // By default, huggingfaceDedicated provider use endpoint-defined-model as placeholder
    if (ProviderConstants.HUGGINGFACE_DEDICATED.equals(provider)) {
      if (modelName == null) {
        modelName = ProviderConstants.HUGGINGFACE_DEDICATED_DEFINED_MODEL;
      } else if (!modelName.equals(ProviderConstants.HUGGINGFACE_DEDICATED_DEFINED_MODEL)) {
        throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
            "'modelName' is not needed for provider %s explicitly, only '%s' is accepted",
            ProviderConstants.HUGGINGFACE_DEDICATED,
            ProviderConstants.HUGGINGFACE_DEDICATED_DEFINED_MODEL);
      }
    }
    this.modelName = modelName;
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
      }
      this.authentication = updatedAuth;
    } else {
      this.authentication = authentication;
    }
    this.parameters = parameters;
  }
}
