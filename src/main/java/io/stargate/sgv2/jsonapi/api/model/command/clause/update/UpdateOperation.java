package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;

/**
 * UpdateOperation represents one of update definitions from {@link UpdateClause} (like {@code $set}
 * or {@code $unset}): single operation can contain multiple actual changes.
 */
public abstract class UpdateOperation {
  /**
   * Method called to apply operation to given document.
   *
   * @param doc Document to apply operation to
   * @return True if document was modified by operation; false if not.
   */
  public abstract boolean updateDocument(ObjectNode doc, UpdateTargetLocator targetLocator);

  /**
   * Shared validation method used by mutating operations (like {@code $set}, {@code $unset}, {@code
   * inc}, {@code pop}) to ensure they are not used to modify paths that are not allowed:
   * specifically Document's primary id, {@code _id}.
   */
  protected static String validateUpdatePath(UpdateOperator oper, String path) {
    if (DocumentConstants.Fields.DOC_ID.equals(path)) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID,
          ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": " + oper.operator());
    }
    return path;
  }

  /**
   * Shared validation method used for update operators that do not accept modifiers: distinction
   * being that modifiers start with "$" character and properties cannot start with it (root level
   * path segment cannot start with it).
   *
   * @param oper Update operator with which path is associated
   * @param path Full (dot notation) path
   * @return Path passed in if valid
   */
  protected static String validateNonModifierPath(UpdateOperator oper, String path) {
    if (looksLikeModifier(path)) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER,
          ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER.getMessage()
              + ": "
              + oper.operator()
              + " does not support modifiers");
    }
    return path;
  }

  protected static boolean looksLikeModifier(String path) {
    return path.startsWith("$");
  }
}
