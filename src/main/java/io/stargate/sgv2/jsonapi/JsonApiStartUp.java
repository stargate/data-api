package io.stargate.sgv2.jsonapi;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.StartupEvent;
import io.stargate.sgv2.api.common.properties.datastore.DataStoreProperties;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class JsonApiStartUp {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonApiStartUp.class);
  private final DataStoreProperties dataStoreProperties;

  @Inject
  public JsonApiStartUp(DataStoreProperties dataStoreProperties) {
    this.dataStoreProperties = dataStoreProperties;
  }

  void onStart(@Observes StartupEvent ev) {
    LOGGER.info(
        String.format("VectorSearch Enabled: %s", dataStoreProperties.vectorSearchEnabled()));
    LOGGER.info(
        String.format(
            "Support Storage Attached Indexing (SAI) Enabled: %s",
            dataStoreProperties.saiEnabled()));
    LOGGER.info(
        String.format("loggedBatches Enabled: %s", dataStoreProperties.loggedBatchesEnabled()));
    LOGGER.info(
        String.format(
            "secondaryIndexes Enabled: %s", dataStoreProperties.secondaryIndexesEnabled()));
    if (!dataStoreProperties.saiEnabled()) {
      LOGGER.warn(
          "Your Cassandra Persistence does not support Storage Attached Indexing (SAI), fail to start the JSONAPI");
      Quarkus.asyncExit();
    }
  }
}
