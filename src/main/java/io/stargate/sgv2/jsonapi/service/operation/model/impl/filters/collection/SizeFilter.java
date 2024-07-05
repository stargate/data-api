package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Filter for document where array has specified number of elements
 */
public class SizeFilter extends MapFilterBase<Integer> {
    public SizeFilter(String path, Operator operator, Integer size) {
        super("array_size", path, operator, size);
        this.indexUsage.arraySizeIndexTag = true;
    }

    @Override
    public JsonNode asJson(JsonNodeFactory nodeFactory) {
        return null;
    }

    @Override
    public boolean canAddField() {
        return false;
    }
}
