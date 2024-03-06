package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public interface CommandSettingsDeserializer {
  CollectionSettings deserialize(
      JsonNode jsonNode, String collectionName, ObjectMapper objectMapper);
}
