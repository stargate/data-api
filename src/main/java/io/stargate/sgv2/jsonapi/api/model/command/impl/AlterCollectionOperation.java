package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.Nullable;

/**
 * Sealed hierarchy of operations for the {@code alterCollection} command.
 *
 * <p>Uses {@link JsonTypeInfo.As#WRAPPER_OBJECT} so the outer key is the operation name, and the
 * inner object is the operation payload.
 *
 * <ul>
 *   <li>{@code "addOpenSearch": {enabled, indexName?, numShards?, numReplicas?, mappings}} — add
 *   <li>{@code "dropOpenSearch": {}} — drop
 * </ul>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = AlterCollectionOperation.AddOpenSearch.class),
  @JsonSubTypes.Type(value = AlterCollectionOperation.DropOpenSearch.class)
})
public interface AlterCollectionOperation {

  /**
   * Add an OpenSearch index to an existing collection.
   *
   * <p>JSON: {@code {"addOpenSearch": {"enabled": true, "mappings": {...}, ...}}}
   */
  @JsonTypeName("addOpenSearch")
  record AddOpenSearch(
      Boolean enabled,
      @Nullable String indexName,
      @Nullable Integer numShards,
      @Nullable Integer numReplicas,
      @Nullable JsonNode mappings)
      implements AlterCollectionOperation {

    /**
     * Converts this payload to the canonical {@link
     * CreateCollectionCommand.Options.OpenSearchDesc}.
     */
    public CreateCollectionCommand.Options.OpenSearchDesc toOpenSearchDesc() {
      return new CreateCollectionCommand.Options.OpenSearchDesc(
          enabled, indexName, numShards, numReplicas, mappings);
    }
  }

  /**
   * Remove the OpenSearch index from an existing collection.
   *
   * <p>JSON: {@code {"dropOpenSearch": {}}}
   */
  @JsonTypeName("dropOpenSearch")
  record DropOpenSearch() implements AlterCollectionOperation {}
}
