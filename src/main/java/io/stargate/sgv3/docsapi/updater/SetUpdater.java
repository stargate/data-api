package io.stargate.sgv3.docsapi.updater;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.commands.clauses.update.SetUpdateOperation;
import io.stargate.sgv3.docsapi.updater.DocumentUpdater.Updater;

/** Implements the $set update operation */
class SetUpdater implements Updater {

  private SetUpdateOperation operation;

  SetUpdater(SetUpdateOperation operation) {
    this.operation = operation;
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    // TODO HACK - only supports top level field setting, does not support foo.bar

    operation
        .updates
        .entrySet()
        .forEach(
            entry -> {
              doc.set(entry.getKey(), entry.getValue());
            });

    return true;
  }
}
