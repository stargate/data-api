package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;

@ApplicationScoped
public class EmbeddingProviderFactory {

  private static Logger logger = org.slf4j.LoggerFactory.getLogger(EmbeddingProviderFactory.class);
  @Inject Instance<EmbeddingProviderConfigStore> embeddingProviderConfigStore;

  private interface ProviderConstructor {
    EmbeddingProvider create(String baseUrl, String apiKey, String modelName);
  }

  private static final Map<String, ProviderConstructor> providersMap =
      Map.ofEntries(
          Map.entry(ProviderConstants.OPENAI, OpenAiEmbeddingClient::new),
          Map.entry(ProviderConstants.HUGGINGFACE, HuggingFaceEmbeddingClient::new),
          Map.entry(ProviderConstants.VERTEXAI, VertexAIEmbeddingClient::new),
          Map.entry(ProviderConstants.COHERE, CohereEmbeddingClient::new),
          Map.entry(ProviderConstants.NVIDIA, NVidiaEmbeddingClient::new));

  public EmbeddingProvider getConfiguration(
      Optional<String> tenant, String serviceName, String modelName) {
    return addService(tenant, serviceName, modelName);
  }

  private synchronized EmbeddingProvider addService(
      Optional<String> tenant, String serviceName, String modelName) {
    final EmbeddingProviderConfigStore.ServiceConfig configuration =
        embeddingProviderConfigStore.get().getConfiguration(tenant, serviceName);

    if (configuration.serviceProvider().equals(ProviderConstants.CUSTOM)) {
      try {
        Optional<Class<?>> clazz = configuration.clazz();
        if (clazz.isPresent()) {
          final EmbeddingProvider customEmbeddingProvider =
              (EmbeddingProvider) clazz.get().getConstructor().newInstance();
          return customEmbeddingProvider;
        } else {
          throw new JsonApiException(
              ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE,
              ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.getMessage() + "custom class undefined");
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new JsonApiException(
            ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE,
            ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.getMessage()
                + "custom class provided does not resolved to EmbeddingProvider "
                + configuration.clazz().get().getCanonicalName());
      }
    }

    return providersMap
        .get(configuration.serviceProvider())
        .create(configuration.baseUrl(), configuration.apiKey(), modelName);
  }
}
