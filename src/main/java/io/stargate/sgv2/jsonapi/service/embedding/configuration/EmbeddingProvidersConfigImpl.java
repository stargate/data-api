package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.smallrye.config.WithConverter;
import io.stargate.embedding.gateway.EmbeddingGateway;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public record EmbeddingProvidersConfigImpl(
    Map<String, EmbeddingProviderConfig> providers, CustomConfig custom)
    implements EmbeddingProvidersConfig {

  public record EmbeddingProviderConfigImpl(
      boolean enabled,
      String url,
      String apiKey,
      List<String> supportedAuthentication,
      List<ParameterConfig> parameters,
      RequestProperties properties,
      List<ModelConfig> models)
      implements EmbeddingProviderConfig {

    public record ModelConfigImpl(
        String name,
        Integer vectorDimension,
        List<ParameterConfig> parameters,
        Map<String, String> properties)
        implements ModelConfig {

      public ModelConfigImpl(
          EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig.ModelConfig grpcModelConfig,
          List<ParameterConfig> modelParameterList) {
        this(
            grpcModelConfig.getName(),
            grpcModelConfig.getVectorDimension(),
            modelParameterList,
            grpcModelConfig.getPropertiesMap());
      }
    }

    public record ParameterConfigImpl(
        String name,
        ParameterType type,
        boolean required,
        Optional<String> defaultValue,
        Optional<String> help)
        implements ParameterConfig {
      public ParameterConfigImpl(
          EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig.ParameterConfig
              grpcModelParameter) {
        this(
            grpcModelParameter.getName(),
            ParameterType.valueOf(grpcModelParameter.getType().name()),
            grpcModelParameter.getRequired(),
            Optional.of(grpcModelParameter.getDefaultValue()),
            Optional.of(grpcModelParameter.getHelp()));
      }
    }

    public record RequestPropertiesImpl(
        int maxRetries, int retryDelayMillis, int requestTimeoutMillis)
        implements RequestProperties {
      public RequestPropertiesImpl(
          EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig.RequestProperties
              grpcProviderConfigProperties) {
        this(
            grpcProviderConfigProperties.getMaxRetries(),
            grpcProviderConfigProperties.getRetryDelayMillis(),
            grpcProviderConfigProperties.getRequestTimeoutMillis());
      }
    }
  }

  public record CustomConfigImpl() implements CustomConfig {
    @Override
    public boolean enabled() {
      return false;
    }

    @Nullable
    @Override
    @WithConverter(ClassNameResolver.class)
    public Optional<Class<?>> clazz() {
      return Optional.empty();
    }
  }
}
