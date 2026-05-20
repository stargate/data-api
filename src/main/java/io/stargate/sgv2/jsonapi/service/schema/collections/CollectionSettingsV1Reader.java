package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.schema.versioning.CollectionSchemaVersion;
import java.util.List;

/**
 * schema_version 1 sample: {"collection":{"name":"newVectorize","schema_version":1,
 * "options":{"indexing":{"deny":["heh"]}, "defaultId":{"type":"objectId"}},
 * "vector":{"dimension":1024,"metric":"cosine","service":{"provider":"nvidia","modelName":"query","authentication":{"type":["HEADER"]},
 * "parameters":{"projectId":"test project"}}} }, "lexical":{"enabled":true,"analyzer":"standard"},
 * "rerank":{"enabled":true,"provider":"nvidia","modelName":"nvidia/llama-3.2-nv-rerankqa-1b-v2"}, }
 */
public class CollectionSettingsV1Reader {

  public CollectionSchemaObject readCollectionSettings(
      RequestContext requestContext,
      JsonNode collectionNode,
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

    //    CollectionLexicalConfig lexicalConfig;
    //    JsonNode lexicalNode =
    //        collectionOptionsNode.path(TableCommentConstants.COLLECTION_LEXICAL_CONFIG_KEY);
    //    if (lexicalNode.isMissingNode()) {
    //      lexicalConfig = CollectionLexicalConfig.configForPreLexical();
    //    } else {
    //      boolean enabled = lexicalNode.path("enabled").asBoolean(false);
    //      JsonNode analyzerNode = lexicalNode.get("analyzer");
    //      lexicalConfig = new CollectionLexicalConfig(enabled, analyzerNode);
    //    }

    CollectionLexicalDef persistedLexical = null;
    var lexicalNode =
        collectionOptionsNode.path(TableCommentConstants.COLLECTION_LEXICAL_CONFIG_KEY);
    if (!lexicalNode.isMissingNode()) {
      // TODO XXX - can we use OBJECT MAPPER ?
      persistedLexical =
          new CollectionLexicalDef(
              lexicalNode.path("enabled").asBoolean(false), lexicalNode.get("analyzer"));
    }

    CollectionRerankDef persistedRerank = null;
    var rerankNode =
        collectionOptionsNode.path(TableCommentConstants.COLLECTION_RERANKING_CONFIG_KEY);
    if (!rerankNode.isMissingNode()) {
      persistedRerank =
          CollectionRerankDef.fromCommentJson(
              tableMetadata.getKeyspace().asInternal(),
              tableMetadata.getName().asInternal(),
              rerankNode,
              objectMapper);
    }

    var schemaVersion = decideSchemaVersion(persistedLexical, persistedRerank);
    return new CollectionSchemaObject(
        requestContext.tenant(),
        tableMetadata,
        idConfig,
        vectorConfig,
        indexingConfig,
        requestContext.versionedSchema().lexicalDef().namedVersion(schemaVersion, persistedLexical),
        requestContext.versionedSchema().rerankDef().namedVersion(schemaVersion, persistedRerank));
  }

  protected CollectionSchemaVersion decideSchemaVersion(
      CollectionLexicalDef persistedLexical, CollectionRerankDef persistedRerank) {

    // XXXX AARON - HACK
    // sanity check, fi we have persisted lexical we should have persisted reranking
    if ((persistedLexical == null) != (persistedRerank == null)) {
      throw new IllegalStateException(
          "Persisted lexical and reranking definitions should be both null or both non-null. Got persistedLexical == null:%s, persistedReranking == null:%s "
              .formatted(persistedLexical == null, persistedRerank == null));
    }

    // IF we have a persisted lexical than we call this version TWO 2 !
    // VERSION 1 was when we had the proper json structure, but did not have the lexical
    return persistedLexical != null ? CollectionSchemaVersion.V_2 : CollectionSchemaVersion.V_1;
  }
}
