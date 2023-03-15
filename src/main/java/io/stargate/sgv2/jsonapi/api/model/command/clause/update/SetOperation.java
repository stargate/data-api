package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of {@code $set} update operation used to assign values to document fields; also
 * used for {@code $setOnInsert} with different configuration.
 */
public class SetOperation extends UpdateOperation<SetOperation.Action> {
  /**
   * Setting to indicate that update should only be applied to inserts (part of upsert), not for
   * updates of existing rows.
   */
  private final boolean onlyOnInsert;

  private SetOperation(List<Action> actions, boolean onlyOnInsert) {
    super(actions);
    this.onlyOnInsert = onlyOnInsert;
  }

  /** Factory method for constructing {@code $set} update operation with given configuration */
  public static SetOperation constructSet(ObjectNode args) {
    return construct(args, false, UpdateOperator.SET);
  }

  /** Override method used to create an operation that $sets a single property */
  public static SetOperation constructSet(String filterPath, JsonNode value) {
    List<Action> additions = new ArrayList<>();
    String path = validateUpdatePath(UpdateOperator.SET, filterPath);
    additions.add(new Action(ActionTargetLocator.forPath(path), value));
    return new SetOperation(additions, false);
  }

  /**
   * Factory method for constructing {@code $setOnInsert} update operation with given configuration
   */
  public static SetOperation constructSetOnInsert(ObjectNode args) {
    return construct(args, true, UpdateOperator.SET_ON_INSERT);
  }

  private static SetOperation construct(
      ObjectNode args, boolean onlyOnInsert, UpdateOperator operator) {
    List<Action> additions = new ArrayList<>();
    var it = args.fields();
    while (it.hasNext()) {
      var entry = it.next();
      String path = validateUpdatePath(operator, entry.getKey());
      additions.add(new Action(ActionTargetLocator.forPath(path), entry.getValue()));
    }
    return new SetOperation(additions, onlyOnInsert);
  }

  @Override
  public boolean shouldApplyIf(boolean isInsert) {
    return isInsert || !onlyOnInsert;
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    boolean modified = false;
    for (Action action : actions) {
      ActionTarget target = action.target().findOrCreate(doc);
      JsonNode newValue = action.value();
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
    return actions.stream().map(Action::path).collect(Collectors.toSet());
  }

  // Just needed for tests
  @Override
  public boolean equals(Object o) {
    return (o instanceof SetOperation) && Objects.equals(this.actions, ((SetOperation) o).actions);
  }

  record Action(ActionTargetLocator target, JsonNode value) implements ActionWithTarget {}
}
