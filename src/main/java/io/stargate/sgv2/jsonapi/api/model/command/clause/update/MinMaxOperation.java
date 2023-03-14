package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.util.JsonNodeComparator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@code $min} and {@code $max} update operation used to modify numeric field
 * values in documents. See {@href
 * https://www.mongodb.com/docs/manual/reference/operator/update/min/} and {@href
 * https://www.mongodb.com/docs/manual/reference/operator/update/max/} for full explanations.
 */
public class MinMaxOperation extends UpdateOperation {
  private final List<MinMaxAction> actions;

  private final boolean isMaxAction;

  private MinMaxOperation(boolean isMaxAction, List<MinMaxAction> actions) {
    this.isMaxAction = isMaxAction;
    this.actions = sortByPath(actions);
  }

  public static MinMaxOperation constructMax(ObjectNode args) {
    return construct(args, UpdateOperator.MAX, true);
  }

  public static MinMaxOperation constructMin(ObjectNode args) {
    return construct(args, UpdateOperator.MIN, false);
  }

  private static MinMaxOperation construct(ObjectNode args, UpdateOperator oper, boolean isMax) {
    Iterator<Map.Entry<String, JsonNode>> fieldIter = args.fields();

    List<MinMaxAction> actions = new ArrayList<>();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      // Verify we do not try to change doc id
      String path = validateUpdatePath(oper, entry.getKey());
      actions.add(new MinMaxAction(UpdateTargetLocator.forPath(path), entry.getValue()));
    }
    return new MinMaxOperation(isMax, actions);
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    // Almost always changes, except if adding zero; need to track
    boolean modified = false;
    for (MinMaxAction action : actions) {
      final JsonNode value = action.value;

      UpdateTarget target = action.target().findOrCreate(doc);
      JsonNode oldValue = target.valueNode();

      if (oldValue == null) { // No such property? Add value
        target.replaceValue(value);
        modified = true;
      } else { // Otherwise, need to see if less-than (min) or greater-than (max)
        if (shouldReplace(oldValue, value)) {
          target.replaceValue(value);
          modified = true;
        }
      }
    }

    return modified;
  }

  private boolean shouldReplace(JsonNode oldValue, JsonNode newValue) {
    if (isMaxAction) {
      // For $max, replace if newValue sorts later
      return JsonNodeComparator.ascending().compare(oldValue, newValue) < 0;
    }
    // For $min, replace if newValue sorts earlier
    return JsonNodeComparator.ascending().compare(oldValue, newValue) > 0;
  }

  /** Value class for per-field update operations. */
  private record MinMaxAction(UpdateTargetLocator target, JsonNode value)
      implements ActionWithTarget {}
}
