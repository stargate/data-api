package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.quarkus.arc.DefaultBean;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Produces;

@Dependent
public class EmbeddingServiceConfigStoreProvider {
  @Produces
  @IfBuildProperty(
      name = "stargate.jsonapi.embedding.service.config.store",
      stringValue = "service")
  public EmbeddingServiceConfigStore service() {
    return InMemoryEmbeddingServiceConfigStore.INSTANCE;
  }

  @Produces
  @IfBuildProperty(
      name = "stargate.jsonapi.embedding.service.config.store",
      stringValue = "inMemoryStore")
  public EmbeddingServiceConfigStore inMemoryStore() {
    return InMemoryEmbeddingServiceConfigStore.INSTANCE;
  }

  @Produces
  @DefaultBean
  public EmbeddingServiceConfigStore property() {
    return InMemoryEmbeddingServiceConfigStore.INSTANCE;
  }
}
