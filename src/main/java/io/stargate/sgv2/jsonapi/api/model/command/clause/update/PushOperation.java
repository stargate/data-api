package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Objects;

public class PushOperation extends UpdateOperation {
  private Map<String, JsonNode> entries;

  private PushOperation(Map<String, JsonNode> entries) {
    this.entries = entries;
  }

  public static PushOperation construct(ObjectNode args) {
    /*
    Iterator<String> it = args.fieldNames();
    List<String> fieldNames = new ArrayList<>();
    while (it.hasNext()) {
      fieldNames.add(validateSetPath(UpdateOperator.UNSET, it.next()));
    }
     */
    return new PushOperation(null);
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    // !!! TODO
    return true;
  }

  // Just needed for tests
  @Override
  public boolean equals(Object o) {
    return (o instanceof PushOperation)
        && Objects.equals(this.entries, ((PushOperation) o).entries);
  }
}
