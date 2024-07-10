package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizer;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import io.stargate.sgv2.jsonapi.util.PathMatch;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Implementation of {@code $set} update operation used to assign values to document fields; also
 * used for {@code $setOnInsert} with different configuration.
 */
public class SetOperation extends UpdateOperation<SetOperation.Action> {
  /**
   * Setting to indicate that update should only be applied to inserts (part of upsert), not for
   * updates of existing rows.
   */
  private final boolean onlyOnInsert;

  private SetOperation(List<Action> actions, boolean onlyOnInsert) {
    super(actions);
    this.onlyOnInsert = onlyOnInsert;
  }

  /** Factory method for constructing {@code $set} update operation with given configuration */
  public static SetOperation constructSet(ObjectNode args) {
    return construct(args, false, UpdateOperator.SET);
  }

  /**
   * Override method used to create an operation that $sets a single property (never used for
   * $setOnInsert)
   */
  public static SetOperation constructSet(String filterPath, JsonNode value) {
    List<Action> additions = new ArrayList<>();
    String path = validateUpdatePath(UpdateOperator.SET, filterPath);
    additions.add(new Action(PathMatchLocator.forPath(path), value));
    return new SetOperation(additions, false);
  }

  /**
   * Factory method for constructing {@code $setOnInsert} update operation with given configuration
   */
  public static SetOperation constructSetOnInsert(ObjectNode args) {
    return construct(args, true, UpdateOperator.SET_ON_INSERT);
  }

  private static SetOperation construct(
      ObjectNode args, boolean onlyOnInsert, UpdateOperator operator) {
    List<Action> additions = new ArrayList<>();
    var it = args.fields();
    while (it.hasNext()) {
      var entry = it.next();
      // 19-May-2023, tatu: As per [json-api#433] need to allow _id override on $setOnInsert
      String path = entry.getKey();
      if (!onlyOnInsert || !DocumentConstants.Fields.DOC_ID.equals(path)) {
        path = validateUpdatePath(operator, path);
      }
      additions.add(new Action(PathMatchLocator.forPath(path), entry.getValue()));
    }
    return new SetOperation(additions, onlyOnInsert);
  }

  @Override
  public boolean shouldApplyIf(boolean isInsert) {
    return isInsert || !onlyOnInsert;
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    boolean modified = false;
    Set<String> setPaths = new HashSet<>();
    actions.stream().forEach(action -> setPaths.add(action.locator().path()));
    for (Action action : actions) {

      if (DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(action.locator().path())) {
        // won't update $vectorize in this method
        // will vectorize on demand and update $vectorize in updateVectorize method below
        continue;
      }

      PathMatch target = action.locator().findOrCreate(doc);
      JsonNode newValue = action.value();
      JsonNode oldValue = target.valueNode();
      // Modify if no old value OR new value differs, as per Mongo-equality rules
      if ((oldValue == null) || !JsonUtil.equalsOrdered(oldValue, newValue)) {
        target.replaceValue(newValue);
        // $vector is updated and $vectorize is not updated, remove the $vectorize field in the
        // document
        if (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(action.locator().path())
            && !setPaths.contains(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
          doc.remove(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
        }
        modified = true;
      }
    }
    return modified;
  }

  /**
   * This updateVectorize method will vectorize as demand and update the $vectorize 1. check if
   * there is diff for $vectorize and proceed 2. vectorize updated $vectorize to get the new vector
   * 3. update $vector and $vectorize
   *
   * @param doc Document to update
   * @param dataVectorizer dataVectorizer
   * @return Uni<Boolean> modified
   */
  public Uni<Boolean> updateVectorize(JsonNode doc, DataVectorizer dataVectorizer) {
    for (Action action : actions) {
      if (DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(action.locator().path())) {
        PathMatch target = action.locator().findOrCreate(doc);
        JsonNode newValue = action.value();
        JsonNode oldValue = target.valueNode();

        if ((oldValue == null) || !JsonUtil.equalsOrdered(oldValue, newValue)) {
          // replace the oldValue with newValue first
          ((ObjectNode) doc).put(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD, newValue);
          // vectorize the newValue, update $vectorize, $vector
          return dataVectorizer.vectorize(List.of(doc), true);
        }
      }
    }
    // no diff for $vectorize, so nothing is modified in this method
    return Uni.createFrom().item(false);
  }

  // Needed because some unit tests check for equality
  @Override
  public boolean equals(Object o) {
    return (o instanceof SetOperation) && Objects.equals(this.actions, ((SetOperation) o).actions);
  }

  record Action(PathMatchLocator locator, JsonNode value) implements ActionWithLocator {}
}
