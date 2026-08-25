package io.stargate.sgv2.jsonapi.service.reranking.configuration;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.runtime.Startup;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.RerankingServiceGrpc;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.ErrorConstants;
import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.util.ClassUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer for {@link RerankingProvidersConfig} that fetches the configuration.
 *
 * <ul>
 *   <li>If the embedding gateway is enabled, it fetches the reranking providers from grpc embedding
 *       gateway service.
 *   <li>If the embedding gateway is disabled, it uses the default reranking provider config.
 * </ul>
 */
public class RerankingProviderConfigProducer {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(RerankingProviderConfigProducer.class);

  @Produces
  @ApplicationScoped
  @Startup
  @Retry(
      maxRetries = 30,
      delay = 6,
      delayUnit = ChronoUnit.SECONDS,
      maxDuration = 180,
      durationUnit = ChronoUnit.SECONDS)
  RerankingProvidersConfig produce(
      DefaultRerankingProviderConfig defaultRerankingProviderConfig,
      OperationsConfig operationsConfig,
      @GrpcClient("embedding") RerankingServiceGrpc.RerankingServiceBlockingStub rerankingService) {
    RerankingProvidersConfig rerankingProvidersConfig = null;

    if (!operationsConfig.enableEmbeddingGateway()) {
      // if the embedding gateway is disabled, use default reranking config/
      LOGGER.info("embedding gateway disabled, use default reranking config");
      rerankingProvidersConfig =
          new RerankingProvidersConfigImpl(defaultRerankingProviderConfig.providers());
    } else {
      // if the embedding gateway is enabled, fetch reranking providers from grpc embedding gateway/
      LOGGER.info(
          "embedding gateway enabled, fetch supported reranking providers from embedding gateway");
      final EmbeddingGateway.GetSupportedRerankingProvidersRequest grpcRequest =
          EmbeddingGateway.GetSupportedRerankingProvidersRequest.newBuilder().build();
      try {
        EmbeddingGateway.GetSupportedRerankingProvidersResponse supportedProvidersResponse =
            rerankingService.getSupportedRerankingProviders(grpcRequest);
        rerankingProvidersConfig = grpcResponseToConfig(supportedProvidersResponse);
      } catch (Exception e) {
        throw EmbeddingProviderException.Code.EMBEDDING_GATEWAY_NOT_AVAILABLE.get(
            Map.of("errorMessage", e.toString()));
      }
    }

    validateRerankingProvidersConfig(rerankingProvidersConfig);

    // Initialize the default reranking provider and model in DefaultRerankingProviderDef as
    // Singleton.
    CollectionRerankDef.initializeDefaultRerankDef(rerankingProvidersConfig);

    return rerankingProvidersConfig;
  }

  /**
   * During startup, Data API should validate the reranking providers config for following rules.
   *
   * <ul>
   *   <li>must have one and only one provider can be default.
   *   <li>must have one and only one model can be default, the model must belongs to the default
   *       provider.
   * </ul>
   */
  private void validateRerankingProvidersConfig(RerankingProvidersConfig rerankingProvidersConfig) {

    // Validate that there is exactly one default provider
    var defaultProviders =
        rerankingProvidersConfig.providers().entrySet().stream()
            .filter(entry -> entry.getValue().isDefault())
            .toList();

    if (defaultProviders.size() != 1) {
      throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(
          Map.of(
              ErrorConstants.TemplateVars.ERROR_CLASS,
              ClassUtils.classSimpleName(getClass()),
              ErrorConstants.TemplateVars.ERROR_MESSAGE,
              "Data API must have exactly one default reranking provider"));
    }

    var defaultProviderEntry = defaultProviders.getFirst();
    var defaultProviderName = defaultProviderEntry.getKey();

    // Collect all default models across all providers
    var allDefaultModels =
        rerankingProvidersConfig.providers().entrySet().stream()
            .flatMap(
                entry ->
                    entry.getValue().models().stream()
                        .filter(
                            RerankingProvidersConfig.RerankingProviderConfig.ModelConfig::isDefault)
                        .map(model -> Map.entry(entry.getKey(), model)))
            .toList();

    if (allDefaultModels.size() != 1) {
      throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(
          Map.of(
              ErrorConstants.TemplateVars.ERROR_CLASS,
              ClassUtils.classSimpleName(getClass()),
              ErrorConstants.TemplateVars.ERROR_MESSAGE,
              "Data API must have exactly one default reranking model"));
    }

    var defaultModelEntry = allDefaultModels.getFirst();
    var defaultModelProviderName = defaultModelEntry.getKey();

    // Ensure the default model belongs to the default provider
    if (!defaultProviderName.equals(defaultModelProviderName)) {
      throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(
          Map.of(
              ErrorConstants.TemplateVars.ERROR_CLASS,
              ClassUtils.classSimpleName(getClass()),
              ErrorConstants.TemplateVars.ERROR_MESSAGE,
              "Default reranking model must belong to the default provider: "
                  + defaultProviderName));
    }
  }

  // package-private for testing the gateway property mapping and its fallbacks
  RerankingProvidersConfig grpcResponseToConfig(
      EmbeddingGateway.GetSupportedRerankingProvidersResponse grpcResponse) {
    Map<String, RerankingProvidersConfig.RerankingProviderConfig> providerMap = new HashMap<>();

    // traverse ProviderConfig in Grpc response
    for (Map.Entry<String, EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig>
        entry : grpcResponse.getSupportedProvidersMap().entrySet()) {
      // create each reranking provider
      providerMap.put(entry.getKey(), createRerankingProviderImpl(entry.getValue()));
    }
    return new RerankingProvidersConfigImpl(providerMap);
  }

  private RerankingProvidersConfig.RerankingProviderConfig createRerankingProviderImpl(
      EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig grpcProviderConfig) {

    Map<
            RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType,
            RerankingProvidersConfig.RerankingProviderConfig.AuthenticationConfig>
        supportedAuthenticationsMap =
            grpcProviderConfig.getSupportedAuthenticationsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        // Convert AuthenticationType string from grpc response to
                        // AuthenticationType
                        eachEntry ->
                            RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType
                                .valueOf(eachEntry.getKey()),
                        // Construct AuthenticationConfig which includes list of tokenConfig
                        eachEntry -> {
                          EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig
                                  .AuthenticationConfig
                              grpcProviderAuthConfig = eachEntry.getValue();
                          List<RerankingProvidersConfig.RerankingProviderConfig.TokenConfig>
                              tokenConfigs =
                                  grpcProviderAuthConfig.getTokensList().stream()
                                      .map(
                                          token ->
                                              new RerankingProvidersConfigImpl
                                                  .RerankingProviderConfigImpl
                                                  .AuthenticationConfigImpl.TokenConfigImpl(
                                                  token.getAccepted(), token.getForwarded()))
                                      .collect(Collectors.toList());
                          return new RerankingProvidersConfigImpl.RerankingProviderConfigImpl
                              .AuthenticationConfigImpl(
                              grpcProviderAuthConfig.getEnabled(), tokenConfigs);
                        }));

    List<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig> models =
        grpcProviderConfig.getModelsList().stream()
            .map(
                model ->
                    new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
                        model.getName(),
                        new ApiModelSupport.ApiModelSupportImpl(
                            ApiModelSupport.SupportStatus.valueOf(
                                model.getApiModelSupport().getStatus()),
                            model.getApiModelSupport().hasMessage()
                                ? Optional.of(model.getApiModelSupport().getMessage())
                                : Optional.empty()),
                        model.getIsDefault(),
                        model.getUrl(),
                        new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
                            .RequestPropertiesImpl(
                            model.getProperties().getAtMostRetries(),
                            model.getProperties().getInitialBackOffMillis(),
                            model.getProperties().getReadTimeoutMillis(),
                            model.getProperties().getMaxBackOffMillis(),
                            model.getProperties().getJitter(),
                            model.getProperties().getMaxBatchSize(),
                            concurrencyPropertyOrDefault(
                                model.getProperties().hasMaxConcurrentBatches(),
                                model.getProperties().getMaxConcurrentBatches(),
                                1,
                                RerankingProvidersConfig.RerankingProviderConfig.ModelConfig
                                    .RequestProperties.DEFAULT_MAX_CONCURRENT_BATCHES),
                            concurrencyPropertyOrDefault(
                                model.getProperties().hasMaxConcurrentCalls(),
                                model.getProperties().getMaxConcurrentCalls(),
                                1,
                                RerankingProvidersConfig.RerankingProviderConfig.ModelConfig
                                    .RequestProperties.DEFAULT_MAX_CONCURRENT_CALLS),
                            concurrencyPropertyOrDefault(
                                model.getProperties().hasMaxQueuedCalls(),
                                model.getProperties().getMaxQueuedCalls(),
                                // 0 is valid here: an explicit "no queueing, fail fast"
                                0,
                                RerankingProvidersConfig.RerankingProviderConfig.ModelConfig
                                    .RequestProperties.DEFAULT_MAX_QUEUED_CALLS),
                            concurrencyPropertyOrDefault(
                                model.getProperties().hasTotalTimeoutMillis(),
                                model.getProperties().getTotalTimeoutMillis(),
                                1,
                                RerankingProvidersConfig.RerankingProviderConfig.ModelConfig
                                    .RequestProperties.DEFAULT_TOTAL_TIMEOUT_MILLIS))))
            .collect(Collectors.toList());

    return new RerankingProvidersConfigImpl.RerankingProviderConfigImpl(
        grpcProviderConfig.getIsDefault(),
        grpcProviderConfig.getDisplayName(),
        grpcProviderConfig.getEnabled(),
        supportedAuthenticationsMap,
        models);
  }

  /**
   * Concurrency-gate properties served by the embedding gateway, falling back to the Data API
   * defaults when the gateway predates the fields or serves an out-of-range value: zero concurrent
   * calls would deadlock every rerank request, so unset must never map to zero.
   */
  private static int concurrencyPropertyOrDefault(
      boolean served, int value, int minValue, String defaultValue) {
    return served && value >= minValue ? value : Integer.parseInt(defaultValue);
  }
}
