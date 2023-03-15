package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Implementation of {@code $unset} update operation used to remove fields from documents. */
public class UnsetOperation extends UpdateOperation<UnsetOperation.Action> {
  private UnsetOperation(List<Action> actions) {
    super(actions);
  }

  public static UnsetOperation construct(ObjectNode args) {
    Iterator<String> it = args.fieldNames();
    List<Action> actions = new ArrayList<>();
    while (it.hasNext()) {
      actions.add(
          new Action(
              ActionTargetLocator.forPath(validateUpdatePath(UpdateOperator.UNSET, it.next()))));
    }
    return new UnsetOperation(actions);
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    boolean modified = false;
    for (Action action : actions) {
      ActionTarget target = action.target().findIfExists(doc);
      modified |= (target.removeValue() != null);
    }
    return modified;
  }

  record Action(ActionTargetLocator target) implements ActionWithTarget {}
}
