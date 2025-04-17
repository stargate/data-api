package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The C* table comment of a data api collection begins to have schema_version number since version
 * 1 Adding this CommandSettingsDeserializer interface, for the convenience of further
 * schema_version changes.
 *
 * <p>CommandSettingsDeserializer works for convert the table comment jsonNode into
 * collectionSettings
 */
@Deprecated
public interface CollectionSettingsReader {

  // TODO: this interface is not used well, see the V0 implementation
  CollectionSchemaObject readCollectionSettings(
      JsonNode jsonNode,
      String keyspaceName,
      String collectionName,
      TableMetadata tableMetadata,
      ObjectMapper objectMapper);
}
