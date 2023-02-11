package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PopOperation extends UpdateOperation {
  private List<PopAction> actions;

  private PopOperation(List<PopAction> actions) {
    this.actions = actions;
  }

  public static PopOperation construct(ObjectNode args) {
    Iterator<Map.Entry<String, JsonNode>> fieldIter = args.fields();

    List<PopAction> actions = new ArrayList<>();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      final String name = entry.getKey();
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
      actions.add(new PopAction(name, first));
    }
    return new PopOperation(actions);
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    boolean changes = false;
    for (PopAction action : actions) {
      final String path = action.path;
      JsonNode node = doc.get(path);
      ArrayNode array;

      if (node == null) { // No such property? Fine, no change
        ;
      } else if (node.isArray()) { // Already array? To modify unless empty
        if (!node.isEmpty()) {
          array = (ArrayNode) node;
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
                + ": $pop requires target to be Array; value at '"
                + path
                + "' of type "
                + node.getNodeType());
      }
    }
    return changes;
  }

  /** Value class for per-field Pop operation definitions. */
  private record PopAction(String path, boolean removeFirst) {}
}
