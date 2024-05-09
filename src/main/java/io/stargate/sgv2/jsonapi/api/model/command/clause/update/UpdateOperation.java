package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * UpdateOperation represents one of update definitions from {@link UpdateClause} (like {@code $set}
 * or {@code $unset}): single operation can contain multiple actual changes.
 */
public abstract class UpdateOperation<A extends ActionWithLocator> {
  protected final List<A> actions;

  protected UpdateOperation(List<A> actions) {
    Collections.sort(actions, NameComparator.INSTANCE);
    this.actions = actions;
  }

  public List<A> actions() {
    return actions;
  }

  /**
   * Method called to apply operation to given document.
   *
   * @param doc Document to apply operation to
   * @return True if document was modified by operation; false if not.
   */
  public abstract boolean updateDocument(ObjectNode doc);

  /**
   * Method called to see if update operator should be applied for specific kind of update:
   * currently one special case is that of document insertion as part of upsert. Most update
   * operations should apply for all updates so the default implementation returns {@code true};
   *
   * @param isInsert True if the document to update was just inserted (as part of upsert operation)
   * @return {@code true} If the update should be applied for document context; {@code false} if it
   *     should be skipped
   */
  public boolean shouldApplyIf(boolean isInsert) {
    return true;
  }

  /**
   * Shared validation method used by mutating operations (like {@code $set}, {@code $unset}, {@code
   * inc}, {@code pop}) to ensure they are not used to modify paths that are not allowed:
   * specifically Document's primary id, {@code _id}.
   */
  protected static String validateUpdatePath(UpdateOperator oper, String path) {
    if (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(path)
        && !(oper.operator().equals("$set")
            || oper.operator().equals("$unset")
            || oper.operator().equals("$setOnInsert"))) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_UPDATE_FOR_VECTOR,
          ErrorCode.UNSUPPORTED_UPDATE_FOR_VECTOR.getMessage() + ": " + oper.operator());
    }

    if (DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(path)
        && !(oper.operator().equals("$set")
            || oper.operator().equals("$unset")
            || oper.operator().equals("$setOnInsert"))) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_UPDATE_FOR_VECTORIZE,
          ErrorCode.UNSUPPORTED_UPDATE_FOR_VECTORIZE.getMessage() + ": " + oper.operator());
    }

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

  static class NameComparator implements Comparator<ActionWithLocator> {
    public static final NameComparator INSTANCE = new NameComparator();

    @Override
    public int compare(ActionWithLocator o1, ActionWithLocator o2) {
      return o1.path().compareTo(o2.path());
    }
  }
}
