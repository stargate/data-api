package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.service.schema.CollectionSchemaVersion;

/** Reads collection settings persisted with schema version V_3. */
public class CollectionSettingsV3Reader extends CollectionSettingsV2Reader {

  @Override
  public CollectionSchemaObject readCollectionSettings(
      RequestContext requestContext,
      JsonNode collectionNode,
      TableMetadata tableMetadata,
      ObjectMapper objectMapper) {
    CollectionSchemaObject v2 =
        super.readCollectionSettings(requestContext, collectionNode, tableMetadata, objectMapper);
    JsonNode openSearchNode =
        collectionNode
            .path(TableCommentConstants.OPTIONS_KEY)
            .path(TableCommentConstants.COLLECTION_OPEN_SEARCH_CONFIG_KEY);
    CollectionOpenSearchDef openSearchDef =
        openSearchNode.isMissingNode()
            ? CollectionOpenSearchDef.fromTableMetadata(tableMetadata, objectMapper)
            : CollectionOpenSearchDef.fromCommentJson(
                openSearchNode,
                tableMetadata.getKeyspace().asInternal(),
                tableMetadata.getName().asInternal(),
                objectMapper);
    return new CollectionSchemaObject(
        requestContext.tenant(),
        tableMetadata,
        v2.idConfig(),
        v2.vectorConfig(),
        v2.indexingConfig(),
        v2.lexicalDefSchemaValue(),
        requestContext
            .schemaRegistry()
            .openSearchDef()
            .namedVersion(CollectionSchemaVersion.V_3, openSearchDef),
        v2.rerankDefSchemaValue());
  }
}
