package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import javax.inject.Inject;

abstract class UpdateOperationTestBase {
  @Inject protected ObjectMapper objectMapper;

  ObjectNode defaultTestDocABC() {
    return objectFromJson(
        """
                    { "a" : 1, "c" : true, "b" : 1234 }
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
}
