package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;

import java.util.List;
import java.util.Optional;

/**
 * Filter for document where array matches (data in same order) as the array in request
 */
public class ArrayEqualsFilter extends MapFilterBase<String> {
    private final List<Object> arrayValue;

    public ArrayEqualsFilter(
            DocValueHasher hasher,
            String path,
            List<Object> arrayData,
            Operator operator) {
        super("query_text_values", path, operator, DBFilterBase.getHash(hasher, arrayData));
        this.arrayValue = arrayData;
        this.indexUsage.textIndexTag = true;
    }

    @Override
    protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
        return Optional.of(toJsonNode(nodeFactory, arrayValue));
    }

    //    @Override
//    public JsonNode asJson(JsonNodeFactory nodeFactory) {
//        return DBFilterBase.getJsonNode(nodeFactory, arrayValue);
//    }
//
//    @Override
//    public boolean canAddField() {
//        return true;
//    }
}
