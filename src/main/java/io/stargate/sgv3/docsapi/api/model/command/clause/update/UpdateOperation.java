package io.stargate.sgv3.docsapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.config.constants.DocumentConstants;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;

/**
 * UpdateOperation represents one of update definitions from {@link UpdateClause} (like {@code $set}
 * or {@code $unset}): single operation can contain multiple actual changes.
 */
public abstract class UpdateOperation {
  public abstract void updateDocument(ObjectNode doc);

  /**
   * Shared validation method used by {@code $set} and {@code $unset} operations to ensure they are
   * not used to modify paths that are not allowed (specifically Document's primary id, {@code
   * _id}).
   */
  protected static String validateSetPath(UpdateOperator oper, String path) {
    if (DocumentConstants.Fields.DOC_ID.equals(path)) {
      throw new DocsException(
          ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID,
          ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": " + oper.operator());
    }
    return path;
  }
}
