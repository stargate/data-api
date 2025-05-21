package io.stargate.sgv2.jsonapi.service.operation.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.builder.ConditionLHS;
import io.stargate.sgv2.jsonapi.service.operation.builder.JsonTerm;
import java.util.Objects;
import java.util.Optional;

/** Filter for logical "$lexical" field in Documents. */
public class MatchCollectionFilter extends CollectionFilter {
  private final String value;

  public MatchCollectionFilter(String path, String value) {
    super(path);
    this.value = Objects.requireNonNull(value, "value must not be null");
    this.collectionIndexUsage.lexicalIndexTag = true;
  }

  @Override
  public BuiltCondition get() {
    return BuiltCondition.of(
        ConditionLHS.column(DocumentConstants.Columns.LEXICAL_INDEX_COLUMN_NAME),
        BuiltConditionPredicate.TEXT_SEARCH,
        new JsonTerm(value));
  }

  protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
    return Optional.of(toJsonNode(nodeFactory, value));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof MatchCollectionFilter other)) return false;
    return Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
