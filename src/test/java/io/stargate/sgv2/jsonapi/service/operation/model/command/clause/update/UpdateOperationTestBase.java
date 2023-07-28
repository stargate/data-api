package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.inject.Inject;
import java.io.IOException;

abstract class UpdateOperationTestBase {
  @Inject protected ObjectMapper objectMapper;

  ObjectNode defaultTestDocABC() {
    return objectFromJson(
        """
                    { "a" : 1, "c" : true, "b" : 1234 }
                    """);
  }

  ObjectNode defaultTestDocABCVector() {
    return objectFromJson(
        """
                        { "a" : 1, "c" : true, "b" : 1234, "$vector" : [0.11, 0.22, 0.33] }
                        """);
  }

  protected ObjectNode objectFromJson(String json) {
    return (ObjectNode) fromJson(json);
  }

  protected JsonNode fromJson(String json) {
    try {
      return objectMapper.readTree(json);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected String asPrettyJson(JsonNode n) {
    try {
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(n);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
