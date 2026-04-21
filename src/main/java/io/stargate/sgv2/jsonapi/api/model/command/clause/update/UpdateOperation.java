package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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
   * @return UpdateOperationResult
   */
  public abstract UpdateOperationResult updateDocument(ObjectNode doc);

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
    switch (path) {
      case DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD:
        switch (oper) {
          case SET, SET_ON_INSERT, UNSET -> {}
          default ->
              throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR_FOR_VECTOR.get(
                  Map.of("operator", oper.apiName()));
        }
        break;
      case DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD:
        switch (oper) {
          case SET, SET_ON_INSERT, UNSET -> {}
          default ->
              throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR_FOR_VECTORIZE.get(
                  Map.of("operator", oper.apiName()));
        }
        break;
      case DocumentConstants.Fields.LEXICAL_CONTENT_FIELD:
        switch (oper) {
          case SET, SET_ON_INSERT, UNSET -> {}
          default ->
              throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR_FOR_LEXICAL.get(
                  Map.of("operator", oper.apiName()));
        }
        break;
      case DocumentConstants.Fields.DOC_ID:
        throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR_FOR_DOC_ID.get(
            Map.of("operator", oper.apiName()));
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
      throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_MODIFIER.get(
          Map.of("errorMessage", "%s does not support modifiers".formatted(oper.apiName())));
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

  /**
   * Abstract method updateDocument will return a UpdateOperationResult. UpdateOperationResult
   * indicated the doc is modified or not, also a List of embeddingUpdateOperation, empty is there
   * is not any embeddingUpdateOperations
   */
  public record UpdateOperationResult(
      boolean modified, List<EmbeddingUpdateOperation> embeddingUpdateOperations) {}
}
