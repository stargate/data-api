package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import io.stargate.sgv2.jsonapi.util.PathMatch;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

  /**
   * Override method used to create an operation that $sets a single property (never used for
   * $setOnInsert)
   */
  public static SetOperation constructSet(String filterPath, JsonNode value) {
    List<Action> additions = new ArrayList<>();
    String path = validateUpdatePath(UpdateOperator.SET, filterPath);
    additions.add(new Action(PathMatchLocator.forPath(path), value));
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
      // 19-May-2023, tatu: As per [json-api#433] need to allow _id override on $setOnInsert
      String path = entry.getKey();
      if (!onlyOnInsert || !DocumentConstants.Fields.DOC_ID.equals(path)) {
        path = validateUpdatePath(operator, path);
      }
      additions.add(new Action(PathMatchLocator.forPath(path), entry.getValue()));
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
      PathMatch target = action.locator().findOrCreate(doc);
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

  // Needed because some unit tests check for equality
  @Override
  public boolean equals(Object o) {
    return (o instanceof SetOperation) && Objects.equals(this.actions, ((SetOperation) o).actions);
  }

  record Action(PathMatchLocator locator, JsonNode value) implements ActionWithLocator {}
}
