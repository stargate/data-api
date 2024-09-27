package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Suppliers;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public record CollectionIndexingConfig(
    Set<String> allowed, Set<String> denied, Supplier<IndexingProjector> indexedProject) {

  // TODO: THIS SHOULD NOT BE A RECORD IF IT CANNOT BE CONSTRUCTED WITH WITHOUT THE INDEXEDPROJECT
  // SUPPLIER
  public CollectionIndexingConfig(Set<String> allowed, Set<String> denied) {
    this(
        allowed,
        denied,
        Suppliers.memoize(() -> IndexingProjector.createForIndexing(allowed, denied)));
  }

  public IndexingProjector indexingProjector() {
    return indexedProject.get();
  }

  public static CollectionIndexingConfig fromJson(JsonNode jsonNode) {
    Set<String> allowed = new HashSet<>();
    Set<String> denied = new HashSet<>();
    if (jsonNode.has("allow")) {
      jsonNode.get("allow").forEach(node -> allowed.add(node.asText()));
    }
    if (jsonNode.has("deny")) {
      jsonNode.get("deny").forEach(node -> denied.add(node.asText()));
    }
    return new CollectionIndexingConfig(allowed, denied);
  }

  // Need to override to prevent comparison of the supplier
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o instanceof CollectionIndexingConfig other) {
      return Objects.equals(this.allowed, other.allowed)
          && Objects.equals(this.denied, other.denied);
    }
    return false;
  }
}
