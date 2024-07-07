package io.stargate.sgv2.jsonapi.service.operation.model.filters.collection;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.DATA_CONTAINS;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.service.operation.model.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.model.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.model.builder.ConditionLHS;
import io.stargate.sgv2.jsonapi.service.operation.model.builder.JsonTerm;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Filter for document where all values exists for an array */
public class AllCollectionFilter extends CollectionFilter {
  private final List<Object> arrayValue;
  private final boolean negation;

  public AllCollectionFilter(String path, List<Object> arrayValue, boolean negation) {
    super(path);
    this.arrayValue = arrayValue;
    this.negation = negation;
  }

  public boolean isNegation() {
    return negation;
  }

  /**
   * DO not update the new document from an upsert for this array operation
   *
   * @param nodeFactory
   * @return
   */
  @Override
  protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
    return Optional.empty();
  }

  //    @Override
  //    public JsonNode asJson(JsonNodeFactory nodeFactory) {
  //        return DBFilterBase.getJsonNode(nodeFactory, arrayValue);
  //    }
  //
  //    @Override
  //    public boolean canAddField() {
  //        return false;
  //    }

  public List<BuiltCondition> getAll() {
    final ArrayList<BuiltCondition> result = new ArrayList<>();
    for (Object value : arrayValue) {
      this.collectionIndexUsage.arrayContainsTag = true;
      result.add(
          BuiltCondition.of(
              ConditionLHS.column(DATA_CONTAINS),
              negation ? BuiltConditionPredicate.NOT_CONTAINS : BuiltConditionPredicate.CONTAINS,
              new JsonTerm(getHashValue(new DocValueHasher(), getPath(), value))));
    }
    return result;
  }

  @Override
  public BuiltCondition get() {
    throw new UnsupportedOperationException("For $all filter we always use getALL() method");
  }
}
