package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Implementation of {@code $set} update operation used to assign values to document fields. */
public class SetOnInsertOperation extends UpdateOperation {
  private final List<SetOnInsertAction> actions;

  private SetOnInsertOperation(List<SetOnInsertAction> actions) {
    this.actions = sortByPath(actions);
  }

  public static SetOnInsertOperation construct(ObjectNode args) {
    List<SetOnInsertAction> additions = new ArrayList<>();
    var it = args.fields();
    while (it.hasNext()) {
      var entry = it.next();
      String path = validateUpdatePath(UpdateOperator.SET, entry.getKey());
      additions.add(new SetOnInsertAction(path, entry.getValue()));
    }
    return new SetOnInsertOperation(additions);
  }

  /**
   * Override method used to set update filter condition fields to the document
   *
   * @param filterPath
   * @param value
   * @return
   */
  public static SetOnInsertOperation construct(String filterPath, JsonNode value) {
    List<SetOnInsertAction> additions = new ArrayList<>();
    String path = validateUpdatePath(UpdateOperator.SET_ON_INSERT, filterPath);
    additions.add(new SetOnInsertAction(path, value));
    return new SetOnInsertOperation(additions);
  }

  @Override
  public boolean shouldApplyIf(boolean isInsert) {
    // Only run for true inserts; skip otherwise
    return isInsert;
  }

  @Override
  public boolean updateDocument(ObjectNode doc, UpdateTargetLocator targetLocator) {
    boolean modified = false;
    for (SetOnInsertAction addition : actions) {
      UpdateTarget target = targetLocator.findOrCreate(doc, addition.path());
      JsonNode newValue = addition.value();
      JsonNode oldValue = target.valueNode();

      // Modify if no old value OR new value differs, as per Mongo-equality rules
      if ((oldValue == null) || !JsonUtil.equalsOrdered(oldValue, newValue)) {
        target.replaceValue(newValue);
        modified = true;
      }
    }
    return modified;
  }

  public Set<String> getPaths() {
    return actions.stream().map(SetOnInsertAction::path).collect(Collectors.toSet());
  }

  // Just needed for tests
  @Override
  public boolean equals(Object o) {
    return (o instanceof SetOnInsertOperation)
        && Objects.equals(this.actions, ((SetOnInsertOperation) o).actions);
  }

  private record SetOnInsertAction(String path, JsonNode value) implements ActionWithPath {}
}
