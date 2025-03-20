package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import java.util.List;

/**
 * schema_version 1 sample: {"collection":{"name":"newVectorize","schema_version":1,
 * "options":{"indexing":{"deny":["heh"]}, "defaultId":{"type":"objectId"}},
 * "vector":{"dimension":1024,"metric":"cosine","service":{"provider":"nvidia","modelName":"query","authentication":{"type":["HEADER"]},
 * "parameters":{"projectId":"test project"}}} }, "lexical":{"enabled":true,"analyzer":"standard"},
 * "rerank":{"enabled":true,"provider":"nvidia","modelName":"nvidia/llama-3.2-nv-rerankqa-1b-v2"}, }
 */
public class CollectionSettingsV1Reader implements CollectionSettingsReader {
  @Override
  public CollectionSchemaObject readCollectionSettings(
      JsonNode collectionNode,
      String keyspaceName,
      String collectionName,
      TableMetadata tableMetadata,
      ObjectMapper objectMapper) {

    JsonNode collectionOptionsNode = collectionNode.get(TableCommentConstants.OPTIONS_KEY);
    // construct collectionSettings VectorConfig
    VectorConfig vectorConfig = VectorConfig.NOT_ENABLED_CONFIG;
    JsonNode vector = collectionOptionsNode.path(TableCommentConstants.COLLECTION_VECTOR_KEY);
    if (!vector.isMissingNode()) {
      VectorColumnDefinition vectorColumnDefinition =
          VectorColumnDefinition.fromJson(vector, objectMapper);
      vectorConfig = VectorConfig.fromColumnDefinitions(List.of(vectorColumnDefinition));
    }
    // construct collectionSettings IndexingConfig
    CollectionIndexingConfig indexingConfig = null;
    JsonNode indexing = collectionOptionsNode.path(TableCommentConstants.COLLECTION_INDEXING_KEY);
    if (!indexing.isMissingNode()) {
      indexingConfig = CollectionIndexingConfig.fromJson(indexing);
    }
    // construct collectionSettings idConfig, default idType as uuid
    final IdConfig idConfig;
    JsonNode idConfigNode = collectionOptionsNode.path(TableCommentConstants.DEFAULT_ID_KEY);
    // should always have idConfigNode in table comment since schema v1
    if (idConfigNode.has("type")) {
      idConfig = new IdConfig(CollectionIdType.fromString(idConfigNode.get("type").asText()));
    } else {
      idConfig = IdConfig.defaultIdConfig();
    }

    CollectionLexicalConfig lexicalConfig;
    JsonNode lexicalNode =
        collectionOptionsNode.path(TableCommentConstants.COLLECTION_LEXICAL_CONFIG_KEY);
    if (lexicalNode == null) {
      lexicalConfig = CollectionLexicalConfig.configForDisabled();
    } else {
      boolean enabled = lexicalNode.path("enabled").asBoolean(false);
      JsonNode analyzerNode = lexicalNode.path("analyzer");
      lexicalConfig = new CollectionLexicalConfig(enabled, analyzerNode);
    }

    CollectionRerankDef rerankingConfig = null;
    JsonNode rerankingNode =
        collectionOptionsNode.path(TableCommentConstants.COLLECTION_RERANKING_CONFIG_KEY);
    if (rerankingNode.isMissingNode()) {
      rerankingConfig = CollectionRerankDef.configForPreRerankingCollections();
    } else {
      rerankingConfig =
          CollectionRerankDef.fromCommentJson(
              keyspaceName, collectionName, rerankingNode, objectMapper);
    }

    return new CollectionSchemaObject(
        keyspaceName,
        collectionName,
        tableMetadata,
        idConfig,
        vectorConfig,
        indexingConfig,
        lexicalConfig,
        rerankingConfig);
  }
}
