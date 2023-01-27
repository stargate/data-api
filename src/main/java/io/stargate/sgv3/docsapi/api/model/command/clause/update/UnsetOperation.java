package io.stargate.sgv3.docsapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class UnsetOperation extends UpdateOperation {
  private List<String> paths;

  private UnsetOperation(List<String> paths) {
    this.paths = paths;
  }

  public static UnsetOperation construct(ObjectNode args) {
    return new UnsetOperation(fieldNames(args));
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
