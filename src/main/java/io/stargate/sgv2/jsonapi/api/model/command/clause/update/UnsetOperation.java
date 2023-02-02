package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class UnsetOperation extends UpdateOperation {
  private List<String> paths;

  private UnsetOperation(List<String> paths) {
    this.paths = paths;
  }

  public static UnsetOperation construct(ObjectNode args) {
    Iterator<String> it = args.fieldNames();
    List<String> fieldNames = new ArrayList<>();
    while (it.hasNext()) {
      fieldNames.add(validateSetPath(UpdateOperator.UNSET, it.next()));
    }
    return new UnsetOperation(fieldNames);
  }

  @Override
  public void updateDocument(ObjectNode doc) {
    paths.stream().forEach(path -> doc.remove(path));
  }

  public Set<String> paths() {
    return new HashSet<>(paths);
  }

  // Just needed for tests
  @Override
  public boolean equals(Object o) {
    return (o instanceof UnsetOperation) && Objects.equals(this.paths, ((UnsetOperation) o).paths);
  }
}
