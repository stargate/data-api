package io.stargate.sgv2.jsonapi.api.v1.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public abstract class TemplateRunner {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  protected TemplateRunner() {}

  public static String asJSON(Object ob) {
    try {
      return MAPPER.writeValueAsString(ob);
    } catch (IOException e) { // should never happen
      throw new RuntimeException(e);
    }
  }

  public static JsonNode asJsonNode(Object ob) {
    return MAPPER.valueToTree(ob);
  }
}
