package io.stargate.sgv3.docsapi.commands.clauses.update;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

public class SetUpdateOperation extends FieldUpdateOperation {

  // TODO HACK - should be JSONPath but Jackson is using this to get reflection
  // and then gets confused by the JSONPath
  public final Map<String, JsonNode> updates;

  public SetUpdateOperation(Map<String, JsonNode> updates) {
    super(FieldUpdateOperator.SET);
    this.updates = updates;
    // updates.entrySet().stream()
    //     .collect(Collectors.toMap(
    //         entry -> JSONPath.from(entry.getKey()),
    //         entry -> entry.getValue()
    //     ));
  }
}
