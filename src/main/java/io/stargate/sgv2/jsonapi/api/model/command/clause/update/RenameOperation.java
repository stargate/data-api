package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
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
    var it = args.properties().iterator();
    while (it.hasNext()) {
      var entry = it.next();
      String srcPath = validateUpdatePath(UpdateOperator.RENAME, entry.getKey());
      JsonNode value = entry.getValue();
      if (!value.isTextual()) {
        throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM.toApiException(
            "$rename requires STRING parameter for 'to', got: %s", value.getNodeType());
      }
      String dstPath = validateUpdatePath(UpdateOperator.RENAME, value.textValue());
      if (srcPath.equals(dstPath)) {
        throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PATH.toApiException(
            "$rename requires that 'source' and `destination` differ ('%s')", srcPath);
      }
      actions.add(new Action(PathMatchLocator.forPath(srcPath), PathMatchLocator.forPath(dstPath)));
    }
    return new RenameOperation(actions);
  }

  @Override
  public UpdateOperationResult updateDocument(ObjectNode doc) {
    boolean modified = false;
    for (Action action : actions) {
      PathMatch src = action.sourceLocator().findIfExists(doc);
      JsonNode value = src.removeValue();
      if (value != null) {
        // $rename does not allow renaming of Array elements
        if (src.contextNode().isArray()) {
          throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PATH.toApiException(
              "$rename does not allow ARRAY field as source ('%s')", action.sourceLocator());
        }

        // If there is a value will be a modification (since source value will
        // disappear), regardless of whether target changes
        modified = true;
        PathMatch dst = action.targetLocator().findOrCreate(doc);

        // Also not allowed: destination as Array element:
        if (dst.contextNode().isArray()) {
          throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PATH.toApiException(
              "$rename does not allow ARRAY field as destination ('%s')", action.targetLocator());
        }

        dst.replaceValue(value);
      }
    }
    return new UpdateOperationResult(modified, List.of());
  }

  // Unlike most operations, we have 2 locators (src, dest), use explicit names
  record Action(PathMatchLocator sourceLocator, PathMatchLocator targetLocator)
      implements ActionWithLocator {
    // Due src, dst names, need to decide which to return as sorting locator;
    // use Source (should not greatly matter)
    public PathMatchLocator locator() {
      return sourceLocator;
    }
  }
}
