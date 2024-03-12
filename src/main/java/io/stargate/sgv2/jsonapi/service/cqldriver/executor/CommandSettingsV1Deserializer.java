package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;

/**
 * schema_version 1 sample:
 * {"collection":{"name":"newVectorize","schema_version":1,"options":{"indexing":{"deny":["heh"]},"vector":{"dimension":1024,"metric":"cosine","service":{"provider":"nvidia","modelName":"query","authentication":{"type":["HEADER"]},"parameters":{"project_id":"test
 * project"}}}}}}
 */
public class CommandSettingsV1Deserializer implements CommandSettingsDeserializer {

  @Override
  public CollectionSettings deserialize(
      JsonNode collectionNode, String collectionName, ObjectMapper objectMapper) {
    JsonNode collectionOptionsNode = collectionNode.get(TableCommentConstants.OPTIONS_KEY);
    CollectionSettings.VectorConfig vectorConfig =
        CollectionSettings.VectorConfig.notEnabledVectorConfig();
    JsonNode vector = collectionOptionsNode.path(TableCommentConstants.COLLECTION_VECTOR_KEY);
    if (!vector.isMissingNode()) {
      vectorConfig = CollectionSettings.VectorConfig.fromJson(vector, objectMapper);
    }
    CollectionSettings.IndexingConfig indexingConfig = null;
    JsonNode indexing = collectionOptionsNode.path(TableCommentConstants.COLLECTION_INDEXING_KEY);
    if (!indexing.isMissingNode()) {
      indexingConfig = CollectionSettings.IndexingConfig.fromJson(indexing);
    }
    return new CollectionSettings(collectionName, vectorConfig, indexingConfig);
  }
}
