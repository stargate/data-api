package io.stargate.sgv3.docsapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** UpdateOperation represents definition of one of update definitions from {@link UpdateClause} */
public abstract class UpdateOperation {
  public abstract void updateDocument(ObjectNode doc);

  protected static List<String> fieldNames(ObjectNode ob) {
    Iterator<String> it = ob.fieldNames();
    List<String> fieldNames = new ArrayList<>();
    while (it.hasNext()) {
      fieldNames.add(it.next());
    }
    return fieldNames;
  }
}
