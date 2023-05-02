package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import io.stargate.sgv2.jsonapi.util.PathMatch;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.List;

/** Implementation of {@code $unset} update operation used to remove fields from documents. */
public class CurrentDateOperation extends UpdateOperation<CurrentDateOperation.Action> {
  private CurrentDateOperation(List<Action> actions) {
    super(actions);
  }

  public static CurrentDateOperation construct(ObjectNode args) {
    List<Action> actions = new ArrayList<>();
    var it = args.fields();
    while (it.hasNext()) {
      var entry = it.next();
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
    throw new JsonApiException(
        ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
        ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
            + ": $currentDate requires argument of either `true` or `{\"$type\":\"date\"}`, got: `"
            + value
            + "`");
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
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
    return modified;
  }

  record Action(PathMatchLocator locator) implements ActionWithLocator {}
}
