package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

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
public interface CommandSettingsDeserializer {
  CollectionSettings deserialize(
      JsonNode jsonNode, String collectionName, ObjectMapper objectMapper);
}
