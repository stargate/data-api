package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.util.PathMatch;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.List;

/** Implementation of {@code $rename} update operation used to rename fields of documents. */
public class RenameOperation extends UpdateOperation<RenameOperation.Action> {
  private RenameOperation(List<Action> actions) {
    super(actions);
  }

  public static RenameOperation construct(ObjectNode args) {
    List<Action> actions = new ArrayList<>();
    var it = args.fields();
    while (it.hasNext()) {
      var entry = it.next();
      String srcPath = validateUpdatePath(UpdateOperator.RENAME, entry.getKey());
      JsonNode value = entry.getValue();
      if (!value.isTextual()) {
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                + ": $rename requires STRING parameter for 'to', got: "
                + value.getNodeType());
      }
      String dstPath = validateUpdatePath(UpdateOperator.RENAME, value.textValue());
      actions.add(new Action(PathMatchLocator.forPath(srcPath), PathMatchLocator.forPath(dstPath)));
    }
    return new RenameOperation(actions);
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    boolean modified = false;
    for (Action action : actions) {
      PathMatch src = action.locator().findIfExists(doc);
      JsonNode value = src.removeValue();
      if (value != null) {
        // If there is a value will be a modification (since source value will
        // disappear), regardless of whether target changes
        modified = true;
        PathMatch dst = action.locator().findOrCreate(doc);
        dst.replaceValue(value);
      }
    }
    return modified;
  }

  record Action(PathMatchLocator locator, PathMatchLocator targetLocator)
      implements ActionWithLocator {}
}
