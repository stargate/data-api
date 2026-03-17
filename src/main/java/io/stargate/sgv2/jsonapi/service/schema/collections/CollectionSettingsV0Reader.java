package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.List;

/**
 * schema_version 0 is before we introduced schema_version into the C* table comment of Data API
 * collection at this version, table comment only works for indexing options. Sample:
 *
 * <pre>
 * {"indexing":{"deny":["address"]}}
 * </pre>
 *
 * <p>Note, all collection created in this schema version 0, should have UUID as idType
 */
public class CollectionSettingsV0Reader {
  public CollectionSchemaObject readCollectionSettings(
      Tenant tenant,
      JsonNode commentConfigNode,
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
        tenant,
        tableMetadata,
        IdConfig.defaultIdConfig(),
        vectorConfig,
        indexingConfig,
        // Legacy config, must assume legacy lexical config (disabled)
        CollectionLexicalConfig.configForPreLexical(),
        // Legacy config, must assume legacy reranking config (disabled)
        CollectionRerankDef.configForPreRerankingCollection());
  }
}
