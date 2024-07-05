package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Filter for document where a field exists or not
 *
 * <p>NOTE: cannot do != null until we get NOT CONTAINS in the DB for set ?????TODO
 */
public class ExistsFilter extends SetFilterBase<String> {
    public ExistsFilter(String path, boolean existFlag) {
        super("exist_keys", path, path, existFlag ? Operator.CONTAINS : Operator.NOT_CONTAINS);
        this.indexUsage.existKeysIndexTag = true;
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
