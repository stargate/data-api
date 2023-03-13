package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@code $mul} update operation used to modify numeric field values in documents.
 * See {@href https://www.mongodb.com/docs/manual/reference/operator/update/mul/} for full
 * explanation.
 */
public class MulOperation extends UpdateOperation {
  private final List<Action> actions;

  private MulOperation(List<Action> actions) {
    this.actions = sortByPath(actions);
  }

  public static MulOperation construct(ObjectNode args) {
    Iterator<Map.Entry<String, JsonNode>> fieldIter = args.fields();

    List<Action> updates = new ArrayList<>();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      // Verify we have neither operators...
      String name = validateNonModifierPath(UpdateOperator.MUL, entry.getKey());
      // nor try to change doc id
      name = validateUpdatePath(UpdateOperator.MUL, name);
      JsonNode value = entry.getValue();
      if (!value.isNumber()) {
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                + ": $mul requires numeric parameter, got: "
                + value.getNodeType());
      }
      updates.add(new Action(name, (NumericNode) value));
    }
    return new MulOperation(updates);
  }

  @Override
  public boolean updateDocument(ObjectNode doc, UpdateTargetLocator targetLocator) {
    boolean modified = false;
    for (Action action : actions) {
      final String path = action.path;
      final NumericNode multiplier = action.value;

      UpdateTarget target = targetLocator.findOrCreate(doc, path);
      JsonNode oldValue = target.valueNode();

      if (oldValue == null) { // No such property? Initialize as zero
        target.replaceValue(doc.numberNode(BigDecimal.ZERO));
        modified = true;
      } else if (oldValue.isNumber()) { // Otherwise, if existing number, can modify
        JsonNode newValue = multiply(doc, oldValue, multiplier);
        // Method will return oldValue if no change needed; identity check is enough
        if (newValue != oldValue) {
          target.replaceValue(newValue);
          modified = true;
        }
      } else { // Non-number existing value? Fail
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET.getMessage()
                + ": $mul requires target to be Number; value at '"
                + path
                + "' of type "
                + oldValue.getNodeType());
      }
    }

    return modified;
  }

  private JsonNode multiply(ObjectNode doc, JsonNode oldValue, JsonNode multiplierValue) {
    BigDecimal multiplier = multiplierValue.decimalValue();
    BigDecimal old = oldValue.decimalValue();

    // Short-cuts to avoid calculation in case old value was 0 (could check if multiplier
    // is 1 but that seems less common)
    if (BigDecimal.ZERO.equals(old)) {
      return oldValue;
    }
    // Let's check equality here to avoid constructing new DecimalNode if not needed:
    BigDecimal newNumber = old.multiply(multiplier);
    if (newNumber.equals(old)) {
      return oldValue;
    }
    return doc.numberNode(newNumber);
  }

  /** Value class for per-field update operations. */
  private record Action(String path, NumericNode value) implements ActionWithPath {}
}
