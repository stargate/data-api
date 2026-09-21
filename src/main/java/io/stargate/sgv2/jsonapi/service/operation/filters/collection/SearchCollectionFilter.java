package io.stargate.sgv2.jsonapi.service.operation.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.builder.ConditionLHS;
import io.stargate.sgv2.jsonapi.service.operation.builder.JsonTerm;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata;
import java.util.Objects;
import java.util.Optional;

/** Filter for the logical "$search" field in Documents; routes the query to OpenSearch. */
public class SearchCollectionFilter extends CollectionFilter {
  private final String value;

  public SearchCollectionFilter(String path, String value) {
    super(path);
    this.value = Objects.requireNonNull(value, "value must not be null");
    this.collectionIndexUsage.openSearchIndexTag = true;
  }

  @Override
  public BuiltCondition get() {
    return BuiltCondition.of(
        ConditionLHS.column(SuperShreddingMetadata.Names.DOC_JSON),
        BuiltConditionPredicate.TEXT_SEARCH,
        new JsonTerm(value));
  }

  protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
    return Optional.of(toJsonNode(nodeFactory, value));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof SearchCollectionFilter other)) return false;
    return Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
