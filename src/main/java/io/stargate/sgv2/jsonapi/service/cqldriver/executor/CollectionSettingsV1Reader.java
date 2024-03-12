package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;

/**
 * schema_version 1 sample:
 * {"collection":{"name":"newVectorize","schema_version":1,"options":{"indexing":{"deny":["heh"]},"vector":{"dimension":1024,"metric":"cosine","service":{"provider":"nvidia","model_name":"query","authentication":{"type":["HEADER"]},"parameters":{"project_id":"test
 * project"}}}}}}
 */
public class CollectionSettingsV1Reader implements CollectionSettingsReader {

  @Override
  public CollectionSettings readCollectionSettings(
      JsonNode collectionNode, String collectionName, ObjectMapper objectMapper) {
    JsonNode collectionOptionsNode = collectionNode.get(TableCommentConstants.OPTIONS_KEY);
    // construct collectionSettings VectorConfig
    CollectionSettings.VectorConfig vectorConfig =
        CollectionSettings.VectorConfig.notEnabledVectorConfig();
    JsonNode vector = collectionOptionsNode.path(TableCommentConstants.COLLECTION_VECTOR_KEY);
    if (!vector.isMissingNode()) {
      vectorConfig = CollectionSettings.VectorConfig.fromJson(vector, objectMapper);
    }
    // construct collectionSettings IndexingConfig
    CollectionSettings.IndexingConfig indexingConfig = null;
    JsonNode indexing = collectionOptionsNode.path(TableCommentConstants.COLLECTION_INDEXING_KEY);
    if (!indexing.isMissingNode()) {
      indexingConfig = CollectionSettings.IndexingConfig.fromJson(indexing);
    }
    // construct collectionSettings idConfig, default idType as uuid
    final CollectionSettings.IdConfig idConfig;
    JsonNode idConfigNode = collectionOptionsNode.path(TableCommentConstants.DEFAULT_ID_KEY);
    // should always have idConfigNode in table comment since schema v1
    if (idConfigNode.has("type")) {
      idConfig =
          new CollectionSettings.IdConfig(
              CollectionSettings.IdType.fromString(idConfigNode.get("type").asText()));
    } else {
      idConfig = CollectionSettings.IdConfig.defaultIdConfig();
    }

    return new CollectionSettings(collectionName, idConfig, vectorConfig, indexingConfig);
  }
}
