package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@code $inc} update operation used to modify numeric field values in documents.
 * See {@href https://www.mongodb.com/docs/manual/reference/operator/update/inc/} for full
 * explanation.
 */
public class IncOperation extends UpdateOperation<IncOperation.Action> {
  private IncOperation(List<Action> actions) {
    super(actions);
  }

  public static IncOperation construct(ObjectNode args) {
    Iterator<Map.Entry<String, JsonNode>> fieldIter = args.fields();

    List<Action> updates = new ArrayList<>();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      // Verify we have neither operators...
      String name = validateNonModifierPath(UpdateOperator.INC, entry.getKey());
      // nor try to change doc id
      name = validateUpdatePath(UpdateOperator.INC, name);
      JsonNode value = entry.getValue();
      if (!value.isNumber()) {
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                + ": $inc requires numeric parameter, got: "
                + value.getNodeType());
      }
      updates.add(new Action(ActionTargetLocator.forPath(name), (NumericNode) value));
    }
    return new IncOperation(updates);
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    // Almost always changes, except if adding zero; need to track
    boolean modified = false;
    for (Action action : actions) {
      final NumericNode toAdd = action.value;

      ActionTarget target = action.target().findOrCreate(doc);
      JsonNode oldValue = target.valueNode();

      if (oldValue == null) { // No such property? Add number
        target.replaceValue(toAdd);
        modified = true;
      } else if (oldValue.isNumber()) { // Otherwise, if existing number, can modify
        // One minor optimization: plain old zero won't change value so can avoid
        // unnecessary mutation.
        if (toAdd.isFloatingPointNumber() || !toAdd.canConvertToInt() || toAdd.intValue() != 0) {
          JsonNode newValue = addNumbers(doc, (NumericNode) oldValue, toAdd);
          target.replaceValue(newValue);
          modified |= !newValue.equals(oldValue);
        }
      } else { // Non-number existing value? Fail
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET.getMessage()
                + ": $inc requires target to be Number; value at '"
                + target.fullPath()
                + "' of type "
                + oldValue.getNodeType());
      }
    }

    return modified;
  }

  private JsonNode addNumbers(ObjectNode doc, NumericNode nr1, NumericNode nr2) {
    // If either floating-point, handle as BigDecimal
    if (nr1.isFloatingPointNumber() || nr2.isFloatingPointNumber()) {
      return doc.numberNode(nr1.decimalValue().add(nr2.decimalValue()));
    }
    // Otherwise use BigInteger to avoid overflows (may optimize in future if necessary;
    // can easily detect int/longs but need to check for overflow)
    return doc.numberNode(nr1.bigIntegerValue().add(nr2.bigIntegerValue()));
  }

  /** Value class for per-field update operations. */
  record Action(ActionTargetLocator target, NumericNode value) implements ActionWithTarget {}
}
