package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.util.PathMatch;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PopOperation extends UpdateOperation<PopOperation.Action> {
  private PopOperation(List<Action> actions) {
    super(actions);
  }

  public static PopOperation construct(ObjectNode args) {
    Iterator<Map.Entry<String, JsonNode>> fieldIter = args.fields();

    List<Action> actions = new ArrayList<>();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      final String path = validateUpdatePath(UpdateOperator.POP, entry.getKey());

      final JsonNode arg = entry.getValue();

      // Argument must be -1 (remove first) or 1 (remove last)
      if (!arg.isNumber()) {
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                + ": $pop requires NUMBER argument (-1 or 1), instead got: "
                + arg.getNodeType());
      }
      boolean first;

      switch (arg.intValue()) {
        case -1:
          first = true;
          break;
        case 1:
          first = false;
          break;
        default:
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $pop requires argument of -1 or 1, instead got: "
                  + arg.intValue());
      }
      actions.add(new Action(PathMatchLocator.forPath(path), first));
    }
    return new PopOperation(actions);
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    boolean changes = false;
    for (Action action : actions) {
      PathMatch target = action.locator().findIfExists(doc);

      JsonNode value = target.valueNode();
      // If target does not match, nothing to do; not an error
      if (value == null) {
        continue;
      }

      // Otherwise must be an array
      if (value.isArray()) { // Already array? To modify unless empty
        if (!value.isEmpty()) {
          ArrayNode array = (ArrayNode) value;
          if (action.removeFirst) {
            array.remove(0);
          } else {
            array.remove(array.size() - 1);
          }
          changes = true;
        }
      } else { // Something else? fail
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET.getMessage()
                + ": $pop requires target to be ARRAY; value at '"
                + target.fullPath()
                + "' of type "
                + value.getNodeType());
      }
    }
    return changes;
  }

  /** Value class for per-field Pop operation definitions. */
  record Action(PathMatchLocator locator, boolean removeFirst) implements ActionWithLocator {}
}
