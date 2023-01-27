package io.stargate.sgv3.docsapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SetOperation extends UpdateOperation {
  private final List<SetAction> additions;

  private SetOperation(List<SetAction> additions) {
    this.additions = additions;
  }

  public static SetOperation construct(ObjectNode args) {
    List<SetAction> additions = new ArrayList<>();
    var it = args.fields();
    while (it.hasNext()) {
      var entry = it.next();
      additions.add(new SetAction(entry.getKey(), entry.getValue()));
    }
    return new SetOperation(additions);
  }

  @Override
  public void updateDocument(ObjectNode doc) {
    additions.forEach(addition -> doc.set(addition.path, addition.value));
  }

  // Just needed for tests
  @Override
  public boolean equals(Object o) {
    return (o instanceof SetOperation)
        && Objects.equals(this.additions, ((SetOperation) o).additions);
  }

  private record SetAction(String path, JsonNode value) {}
}
