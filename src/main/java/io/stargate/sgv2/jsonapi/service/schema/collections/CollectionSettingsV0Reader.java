package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.List;

/**
 * schema_version 0 is before we introduce schema_version into the C* table comment of data api
 * collection at this version, table comment only works for indexing options sample:
 * {"indexing":{"deny":["address"]}}
 *
 * <p>Note, all collection created in this schema version 0, should have UUID as idType
 */
public class CollectionSettingsV0Reader implements CollectionSettingsReader {

  // TODO: Why have  function with the same name as the interface method ?
  public CollectionSchemaObject readCollectionSettings(
      JsonNode commentConfigNode,
      String keyspaceName,
      String collectionName,
      TableMetadata tableMetadata,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction function,
      EmbeddingSourceModel sourceModel) {

    VectorConfig vectorConfig =
        vectorEnabled
            ? VectorConfig.fromColumnDefinitions(
                List.of(
                    new VectorColumnDefinition(
                        DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                        vectorSize,
                        function,
                        sourceModel,
                        null)))
            : VectorConfig.NOT_ENABLED_CONFIG;
    CollectionIndexingConfig indexingConfig = null;
    JsonNode indexing = commentConfigNode.path(TableCommentConstants.COLLECTION_INDEXING_KEY);
    if (!indexing.isMissingNode()) {
      indexingConfig = CollectionIndexingConfig.fromJson(indexing);
    }
    return new CollectionSchemaObject(
        keyspaceName,
        collectionName,
        tableMetadata,
        IdConfig.defaultIdConfig(),
        vectorConfig,
        indexingConfig,
        // Legacy config, must assume legacy lexical config (disabled)
        CollectionLexicalConfig.configForDisabled(),
        // Legacy config, must assume legacy reranking config (disabled)
        CollectionRerankDef.configForPreRerankingCollections());
  }

  /**
   * schema v0 is obsolete(supported though for backwards compatibility, hard to implement
   * readCollectionSettings method based on interface method signature
   */
  @Override
  public CollectionSchemaObject readCollectionSettings(
      JsonNode jsonNode,
      String keyspaceName,
      String collectionName,
      TableMetadata tableMetadata,
      ObjectMapper objectMapper) {
    // TODO: this is really confusing, why does this implement the interface and not implement the
    // one method ?
    return null;
  }
}
