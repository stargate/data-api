package io.stargate.sgv2.jsonapi.fixtures.data;

import com.datastax.oss.driver.api.core.type.DataType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/** All values are returned as null */
public class AllNullValues extends DefaultData {
  @Override
  protected JsonNode getJsonNode(DataType type) {
    return JsonNodeFactory.instance.nullNode();
  }
}
