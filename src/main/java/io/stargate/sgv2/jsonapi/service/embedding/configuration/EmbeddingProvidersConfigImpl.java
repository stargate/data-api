package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.stargate.embedding.gateway.EmbeddingGateway;
import java.util.*;
import java.util.stream.Collectors;

public record EmbeddingProvidersConfigImpl(
    Map<String, EmbeddingProviderConfig> providers, CustomConfig custom)
    implements EmbeddingProvidersConfig {

  public record EmbeddingProviderConfigImpl(
      String displayName,
      boolean enabled,
      Optional<String> url,
      Map<AuthenticationType, AuthenticationConfig> supportedAuthentications,
      List<ParameterConfig> parameters,
      RequestProperties properties,
      List<ModelConfig> models)
      implements EmbeddingProviderConfig {

    public record AuthenticationConfigImpl(boolean enabled, List<TokenConfig> tokens)
        implements AuthenticationConfig {

      public record TokenConfigImpl(String accepted, String forwarded) implements TokenConfig {}
    }

    public record ModelConfigImpl(
        String name,
        Optional<Integer> vectorDimension,
        List<ParameterConfig> parameters,
        Map<String, String> properties,
        Optional<String> serviceUrlOverride)
        implements ModelConfig {

      public ModelConfigImpl(
          EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig.ModelConfig grpcModelConfig,
          List<ParameterConfig> modelParameterList) {
        this(
            grpcModelConfig.getName(),
            grpcModelConfig.hasVectorDimension()
                ? Optional.of(grpcModelConfig.getVectorDimension())
                : Optional.empty(),
            modelParameterList,
            grpcModelConfig.getPropertiesMap(),
            grpcModelConfig.hasServiceUrlOverride()
                ? Optional.of(grpcModelConfig.getServiceUrlOverride())
                : Optional.empty());
      }
    }

    public record ParameterConfigImpl(
        String name,
        ParameterType type,
        boolean required,
        Optional<String> defaultValue,
        Map<ValidationType, List<Integer>> validation,
        Optional<String> help,
        Optional<String> displayName,
        Optional<String> hint)
        implements ParameterConfig {
      public ParameterConfigImpl(
          EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig.ParameterConfig
              grpcModelParameter) {
        this(
            grpcModelParameter.getName(),
            ParameterType.valueOf(grpcModelParameter.getType().name()),
            grpcModelParameter.getRequired(),
            Optional.of(grpcModelParameter.getDefaultValue()),
            grpcModelParameter.getValidationMap().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        e -> ValidationType.fromString(e.getKey()),
                        e -> new ArrayList<>(e.getValue().getValuesList()))),
            Optional.of(grpcModelParameter.getHelp()),
            Optional.of(grpcModelParameter.getDisplayName()),
            Optional.of(grpcModelParameter.getHint()));
      }
    }

    public record RequestPropertiesImpl(
        int atMostRetries,
        int initialBackOffMillis,
        int readTimeoutMillis,
        int maxBackOffMillis,
        double jitter,
        Optional<String> maxInputLength,
        Optional<String> taskTypeStore,
        Optional<String> taskTypeRead,
        int maxBatchSize)
        implements RequestProperties {
      public RequestPropertiesImpl(
          EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig.RequestProperties
              grpcProviderConfigProperties) {
        this(
            grpcProviderConfigProperties.getAtMostRetries(),
            grpcProviderConfigProperties.getInitialBackOffMillis(),
            grpcProviderConfigProperties.getReadTimeoutMillis(),
            grpcProviderConfigProperties.getMaxBackOffMillis(),
            grpcProviderConfigProperties.getJitter(),
            Optional.ofNullable(grpcProviderConfigProperties.getMaxInputLength()),
            Optional.ofNullable(grpcProviderConfigProperties.getTaskTypeStore()),
            Optional.ofNullable(grpcProviderConfigProperties.getTaskTypeRead()),
            grpcProviderConfigProperties.getMaxBatchSize());
      }
    }
  }
}
