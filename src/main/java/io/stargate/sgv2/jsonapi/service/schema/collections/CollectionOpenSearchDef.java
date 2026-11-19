package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.SchemaDefaults;
import io.stargate.sgv2.jsonapi.service.schema.SchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.SchemaHolder;
import java.util.Map;

/** Validated OpenSearch replication configuration for a collection. */
public record CollectionOpenSearchDef(
    boolean enabled,
    @JsonInclude(JsonInclude.Include.NON_NULL) String indexName,
    @JsonInclude(JsonInclude.Include.NON_NULL) Integer numShards,
    @JsonInclude(JsonInclude.Include.NON_NULL) Integer numReplicas,
    @JsonInclude(JsonInclude.Include.NON_NULL) JsonNode mappings,
    @JsonInclude(JsonInclude.Include.NON_NULL) String saiIndexName) {

  private static final CollectionOpenSearchDef DISABLED =
      new CollectionOpenSearchDef(false, null, null, null, null, null);

  public static final SchemaDefaults<CollectionOpenSearchDef> SCHEMA_DEFAULTS =
      new SchemaDefaults<>() {
        @Override
        public CollectionOpenSearchDef forPreRelease() {
          return DISABLED;
        }

        @Override
        public CollectionOpenSearchDef currentDefault() {
          return DISABLED;
        }

        @Override
        public CollectionOpenSearchDef forDisabledFeature() {
          return DISABLED;
        }
      };

  public static SchemaHolder<CollectionOpenSearchDef> fromApiDesc(
      CreateCollectionCommand.Options.OpenSearchDesc openSearchDesc,
      String keyspace,
      String collectionName,
      SchemaFactory<CollectionOpenSearchDef> schemaFactory) {
    if (openSearchDesc == null) {
      return schemaFactory.currentVersion(null);
    }

    if (openSearchDesc.enabled() == null) {
      throw invalid("'enabled' is required property for 'openSearch' Object value");
    }

    if (!openSearchDesc.enabled()) {
      // Mirror the lexical pattern: report each offending field individually so the caller knows
      // exactly which property to remove.  Acceptable values when disabled: absent or JSON null.
      if (openSearchDesc.indexName() != null) {
        throw invalid(
            "'openSearch' is disabled, but 'openSearch.indexName' was provided. "
                + "When 'openSearch' is disabled, 'openSearch.indexName' must be omitted.");
      }
      if (openSearchDesc.numShards() != null) {
        throw invalid(
            "'openSearch' is disabled, but 'openSearch.numShards' was provided. "
                + "When 'openSearch' is disabled, 'openSearch.numShards' must be omitted.");
      }
      if (openSearchDesc.numReplicas() != null) {
        throw invalid(
            "'openSearch' is disabled, but 'openSearch.numReplicas' was provided. "
                + "When 'openSearch' is disabled, 'openSearch.numReplicas' must be omitted.");
      }
      if (!mappingsNotDefined(openSearchDesc.mappings())) {
        throw invalid(
            "'openSearch' is disabled, but 'openSearch.mappings' was provided. "
                + "When 'openSearch' is disabled, 'openSearch.mappings' must be omitted.");
      }
      return schemaFactory.currentVersion(DISABLED);
    }

    // 'mappings' is optional: when absent (or an empty object) no custom mapping is applied and
    // OpenSearch uses its own default (dynamic) mapping.
    JsonNode providedMappings = openSearchDesc.mappings();
    JsonNode mappings = null;
    if (providedMappings != null && !mappingsNotDefined(providedMappings)) {
      if (!providedMappings.isObject() || providedMappings.has("_id")) {
        throw SchemaException.Code.OPEN_SEARCH_MISSING_FIELD_MAPPINGS.get();
      }
      providedMappings
          .fieldNames()
          .forEachRemaining(
              fieldName -> {
                if (fieldName.startsWith("$")) {
                  throw SchemaException.Code.OPEN_SEARCH_MISSING_FIELD_MAPPINGS.get();
                }
              });
      mappings = providedMappings;
    }

    String defaultIndexName = "hcd_%s_%s".formatted(keyspace, collectionName);
    String indexName =
        openSearchDesc.indexName() == null ? defaultIndexName : openSearchDesc.indexName();
    return schemaFactory.currentVersion(
        new CollectionOpenSearchDef(
            true,
            indexName,
            openSearchDesc.numShards(),
            openSearchDesc.numReplicas(),
            mappings,
            defaultIndexName));
  }

  public static CollectionOpenSearchDef fromTableMetadata(
      TableMetadata tableMetadata, ObjectMapper objectMapper) {
    return tableMetadata.getIndexes().values().stream()
        .filter(
            index ->
                index.getKind() == IndexKind.CUSTOM
                    && isOpenSearchIndex(index.getOptions().get("class_name")))
        .findFirst()
        .map(index -> fromIndexMetadata(index, objectMapper))
        .orElse(null);
  }

  private static CollectionOpenSearchDef fromIndexMetadata(
      IndexMetadata index, ObjectMapper objectMapper) {
    String indexName = index.getOptions().get("indexName");
    if (indexName == null) {
      throw SchemaException.Code.OPEN_SEARCH_CORRUPT_SCHEMA.get();
    }
    // 'customMappingsJson' is optional: an index created without custom mappings relies on the
    // OpenSearch default mapping.
    JsonNode userMappings = null;
    String customMappingsJson = index.getOptions().get("customMappingsJson");
    if (customMappingsJson != null) {
      try {
        JsonNode mappings = objectMapper.readTree(customMappingsJson).path("properties");
        if (!mappingsNotDefined(mappings) && !mappings.isMissingNode()) {
          if (!mappings.isObject()) {
            throw SchemaException.Code.OPEN_SEARCH_CORRUPT_SCHEMA.get();
          }
          ObjectNode copy = mappings.deepCopy();
          copy.remove("_id");
          userMappings = mappingsNotDefined(copy) ? null : copy;
        }
      } catch (JacksonException e) {
        throw SchemaException.Code.OPEN_SEARCH_CORRUPT_SCHEMA.get();
      }
    }
    return new CollectionOpenSearchDef(
        true,
        indexName,
        integerOption(index, "numShards"),
        integerOption(index, "numReplicas"),
        userMappings,
        index.getName().asInternal());
  }

  public static CollectionOpenSearchDef fromCommentJson(
      JsonNode openSearchNode, String keyspace, String collectionName, ObjectMapper objectMapper) {
    if (!openSearchNode.path("enabled").asBoolean(false)) {
      return DISABLED;
    }
    String indexName = openSearchNode.path("indexName").asText(null);
    if (indexName == null) {
      throw SchemaException.Code.OPEN_SEARCH_CORRUPT_SCHEMA.get();
    }
    JsonNode storedMappings = openSearchNode.get("mappings");
    JsonNode mappings = null;
    if (storedMappings != null && !mappingsNotDefined(storedMappings)) {
      if (!storedMappings.isObject()) {
        throw SchemaException.Code.OPEN_SEARCH_CORRUPT_SCHEMA.get();
      }
      mappings = storedMappings;
    }
    return new CollectionOpenSearchDef(
        true,
        indexName,
        openSearchNode.has("numShards") ? openSearchNode.get("numShards").asInt() : null,
        openSearchNode.has("numReplicas") ? openSearchNode.get("numReplicas").asInt() : null,
        mappings,
        "hcd_%s_%s".formatted(keyspace, collectionName));
  }

  public CreateCollectionCommand.Options.OpenSearchDesc toApiDesc() {
    return new CreateCollectionCommand.Options.OpenSearchDesc(
        enabled(), indexName(), numShards(), numReplicas(), mappings());
  }

  /**
   * Returns {@code true} when user-supplied custom mappings exist (otherwise OpenSearch defaults).
   */
  public boolean hasCustomMappings() {
    return mappings != null;
  }

  /** Must only be called when {@link #hasCustomMappings()} is {@code true}. */
  public ObjectNode customMappings(ObjectMapper objectMapper) {
    ObjectNode properties = objectMapper.createObjectNode();
    mappings().properties().forEach(entry -> properties.set(entry.getKey(), entry.getValue()));
    return objectMapper.createObjectNode().set("properties", properties);
  }

  private static boolean isOpenSearchIndex(String className) {
    return className != null
        && ("OpenSearchIndex".equals(className) || className.endsWith(".OpenSearchIndex"));
  }

  private static Integer integerOption(IndexMetadata index, String optionName) {
    String value = index.getOptions().get(optionName);
    return value == null ? null : Integer.valueOf(value);
  }

  private static boolean mappingsNotDefined(JsonNode mappings) {
    return mappings == null || mappings.isNull() || (mappings.isObject() && mappings.isEmpty());
  }

  private static SchemaException invalid(String message) {
    return SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS.get(Map.of("message", message));
  }
}
