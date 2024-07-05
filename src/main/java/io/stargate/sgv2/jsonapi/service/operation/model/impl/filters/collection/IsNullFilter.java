package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.DBFilterBase;

/**
 * Filter for document where a field == null
 *
 * <p>NOTE: cannot do != null until we get NOT CONTAINS in the DB for set
 */
public class IsNullFilter extends SetFilterBase<String> {
    public IsNullFilter(String path, Operator operator) {
        super("query_null_values", path, path, operator);
        this.indexUsage.nullIndexTag = true;
    }

    @Override
    public JsonNode asJson(JsonNodeFactory nodeFactory) {
        return DBFilterBase.getJsonNode(nodeFactory, null);
    }

    @Override
    public boolean canAddField() {
        return true;
    }
}
