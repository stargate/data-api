package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.service.cql.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.cql.builder.Predicate;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.JsonTerm;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;

import java.util.ArrayList;
import java.util.List;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.DATA_CONTAINS;

/**
 * Filter for document where all values exists for an array
 */
public class AllFilter extends DBFilterBase {
    private final List<Object> arrayValue;
    private final boolean negation;

    public AllFilter(String path, List<Object> arrayValue, boolean negation) {
        super(path);
        this.arrayValue = arrayValue;
        this.negation = negation;
    }

    public boolean isNegation() {
        return negation;
    }

    @Override
    public JsonNode asJson(JsonNodeFactory nodeFactory) {
        return DBFilterBase.getJsonNode(nodeFactory, arrayValue);
    }

    @Override
    public boolean canAddField() {
        return false;
    }

    public List<BuiltCondition> getAll() {
        final ArrayList<BuiltCondition> result = new ArrayList<>();
        for (Object value : arrayValue) {
            this.indexUsage.arrayContainsTag = true;
            result.add(
                    BuiltCondition.of(
                            BuiltCondition.LHS.column(DATA_CONTAINS),
                            negation ? Predicate.NOT_CONTAINS : Predicate.CONTAINS,
                            new JsonTerm(getHashValue(new DocValueHasher(), getPath(), value))));
        }
        return result;
    }

    @Override
    public BuiltCondition get() {
        throw new UnsupportedOperationException("For $all filter we always use getALL() method");
    }
}
