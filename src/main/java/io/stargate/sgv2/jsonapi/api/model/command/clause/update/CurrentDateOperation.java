package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import io.stargate.sgv2.jsonapi.util.PathMatch;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@code currentDate} update operation used to set field(s) to the current server
 * time (at time of update).
 */
public class CurrentDateOperation extends UpdateOperation<CurrentDateOperation.Action> {
  private CurrentDateOperation(List<Action> actions) {
    super(actions);
  }

  public static CurrentDateOperation construct(ObjectNode args) {
    List<Action> actions = new ArrayList<>();
    for (var entry : args.properties()) {
      String path = validateUpdatePath(UpdateOperator.CURRENT_DATE, entry.getKey());
      // Validate that we either have boolean `true` or Object `{ "$type" : "date" }
      verifyIsTrueOrDate(entry.getValue());
      actions.add(new Action(PathMatchLocator.forPath(path)));
    }
    return new CurrentDateOperation(actions);
  }

  private static void verifyIsTrueOrDate(JsonNode value) {
    if (value.isObject()) {
      if (value.size() == 1) {
        JsonNode v2 = value.path("$type");
        if (v2.isTextual() && "date".equals(v2.textValue())) {
          return;
        }
      }
    } else if (value.isBoolean() && value.booleanValue()) {
      return;
    }
    throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PARAM.get(
        Map.of(
            "errorMessage",
            "$currentDate requires argument of either `true` or `{\"$type\":\"date\"}`, got: `%s`"
                .formatted(value)));
  }

  @Override
  public UpdateOperationResult updateDocument(ObjectNode doc) {
    boolean modified = false;
    final long now = System.currentTimeMillis();
    ObjectNode newValue = JsonUtil.createEJSonDate(doc, now);
    for (Action action : actions) {
      PathMatch target = action.locator().findOrCreate(doc);
      JsonNode oldValue = target.valueNode();

      // Modify if no old value OR new value differs, as per Mongo-equality rules
      if ((oldValue == null) || !JsonUtil.equalsOrdered(oldValue, newValue)) {
        target.replaceValue(newValue);
        modified = true;
      }
    }
    return new UpdateOperationResult(modified, List.of());
  }

  record Action(PathMatchLocator locator) implements ActionWithLocator {}
}
