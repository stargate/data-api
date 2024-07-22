package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;

/**
 * schema_version 1 sample:
 * {"collection":{"name":"newVectorize","schema_version":1,"options":{"indexing":{"deny":["heh"]},"defaultId":{"type":"objectId"}},"vector":{"dimension":1024,"metric":"cosine","service":{"provider":"nvidia","modelName":"query","authentication":{"type":["HEADER"]},"parameters":{"projectId":"test
 * project"}}}}}}
 */
public class CollectionSettingsV1Reader implements CollectionSettingsReader {
  @Override
  public CollectionSchemaObject readCollectionSettings(
      JsonNode collectionNode,
      String keyspaceName,
      String collectionName,
      ObjectMapper objectMapper) {

    JsonNode collectionOptionsNode = collectionNode.get(TableCommentConstants.OPTIONS_KEY);
    // construct collectionSettings VectorConfig
    VectorConfig vectorConfig = VectorConfig.notEnabledVectorConfig();
    JsonNode vector = collectionOptionsNode.path(TableCommentConstants.COLLECTION_VECTOR_KEY);
    if (!vector.isMissingNode()) {
      vectorConfig = VectorConfig.fromJson(vector, objectMapper);
    }
    // construct collectionSettings IndexingConfig
    CollectionSchemaObject.IndexingConfig indexingConfig = null;
    JsonNode indexing = collectionOptionsNode.path(TableCommentConstants.COLLECTION_INDEXING_KEY);
    if (!indexing.isMissingNode()) {
      indexingConfig = CollectionSchemaObject.IndexingConfig.fromJson(indexing);
    }
    // construct collectionSettings idConfig, default idType as uuid
    final CollectionSchemaObject.IdConfig idConfig;
    JsonNode idConfigNode = collectionOptionsNode.path(TableCommentConstants.DEFAULT_ID_KEY);
    // should always have idConfigNode in table comment since schema v1
    if (idConfigNode.has("type")) {
      idConfig =
          new CollectionSchemaObject.IdConfig(
              CollectionSchemaObject.IdType.fromString(idConfigNode.get("type").asText()));
    } else {
      idConfig = CollectionSchemaObject.IdConfig.defaultIdConfig();
    }

    return new CollectionSchemaObject(
        keyspaceName, collectionName, idConfig, vectorConfig, indexingConfig);
  }
}
