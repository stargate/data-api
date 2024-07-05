package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Filters db documents based on a boolean field value
 */
public class BoolFilter extends MapFilterBase<Boolean> {
    private final boolean boolValue;

    public BoolFilter(String path, Operator operator, Boolean value) {
        super("query_bool_values", path, operator, value);
        this.boolValue = value;
        if (Operator.EQ == operator || Operator.NE == operator) indexUsage.arrayContainsTag = true;
        else indexUsage.booleanIndexTag = true;
    }

    @Override
    public JsonNode asJson(JsonNodeFactory nodeFactory) {
        return nodeFactory.booleanNode(boolValue);
    }

    @Override
    public boolean canAddField() {
        return Operator.EQ.equals(operator);
    }
}
