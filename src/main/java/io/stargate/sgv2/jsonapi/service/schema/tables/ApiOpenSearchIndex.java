package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.table.IndexDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.OpenSearchIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.IndexFactoryFromCql;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.IndexFactoryFromIndexDesc;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An index of type {@link ApiIndexType#OPEN_SEARCH} that replicates one or more table columns to an
 * OpenSearch cluster managed by HCD.
 *
 * <p>Unlike SAI / text / vector indexes, this index covers <b>multiple</b> columns (or all non-PK
 * columns when none are specified). It therefore does <b>not</b> extend {@link ApiSupportedIndex}
 * (which assumes a single {@code targetColumn}); instead it directly implements {@link
 * ApiIndexDef}.
 *
 * <p>The <em>SAI index name</em> is the Cassandra custom index identifier — the name supplied by
 * the caller in {@code createOpenSearchIndex.name} and used in every subsequent {@code
 * expr(<saiIndexName>, ...)} CQL predicate.
 */
public class ApiOpenSearchIndex implements ApiIndexDef {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApiOpenSearchIndex.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Factory that creates an {@link ApiOpenSearchIndex} from a user {@code createOpenSearchIndex}
   * command desc.
   */
  public static final IndexFactoryFromIndexDesc<ApiOpenSearchIndex, OpenSearchIndexDefinitionDesc>
      FROM_DESC_FACTORY = new UserDescFactory();

  /** Factory that creates an {@link ApiOpenSearchIndex} from CQL {@link IndexMetadata}. */
  public static final IndexFactoryFromCql FROM_CQL_FACTORY = new CqlTypeFactory();

  /**
   * Convenience factory method that creates an {@link ApiOpenSearchIndex} directly from Cassandra
   * {@link IndexMetadata} — used by {@link
   * io.stargate.sgv2.jsonapi.service.schema.tables.factories.IndexFactoryFromCql#create}.
   */
  public static ApiOpenSearchIndex fromCqlMetadata(IndexMetadata indexMetadata) {
    Objects.requireNonNull(indexMetadata, "indexMetadata must not be null");
    return new ApiOpenSearchIndex(
        indexMetadata.getName(), List.of(), new HashMap<>(indexMetadata.getOptions()));
  }

  // ── Fields ──────────────────────────────────────────────────────────────────

  /** The Cassandra custom index name (SAI index name). Used in {@code expr(<name>, ...)}. */
  private final CqlIdentifier indexName;

  /** Columns covered by this index. Empty list = all non-PK columns (HCD default). */
  private final List<CqlIdentifier> columns;

  /** Raw CQL {@code WITH OPTIONS} map. */
  private final Map<String, String> indexOptions;

  // ── Constructor ─────────────────────────────────────────────────────────────

  private ApiOpenSearchIndex(
      CqlIdentifier indexName, List<CqlIdentifier> columns, Map<String, String> indexOptions) {
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    this.columns =
        Collections.unmodifiableList(Objects.requireNonNull(columns, "columns must not be null"));
    this.indexOptions =
        Collections.unmodifiableMap(
            Objects.requireNonNull(indexOptions, "indexOptions must not be null"));
  }

  // ── ApiIndexDef interface ────────────────────────────────────────────────────

  @Override
  public CqlIdentifier indexName() {
    return indexName;
  }

  /**
   * OpenSearch indexes do not target a single column — returns {@code null}.
   *
   * <p>Callers that need the column list should use {@link #columns()} instead.
   */
  @Override
  public CqlIdentifier targetColumn() {
    return null;
  }

  @Override
  public ApiIndexType indexType() {
    return ApiIndexType.OPEN_SEARCH;
  }

  /** OpenSearch indexes have no index function. */
  @Override
  public ApiIndexFunction indexFunction() {
    return null;
  }

  @Override
  public Map<String, String> indexOptions() {
    return indexOptions;
  }

  /**
   * The columns this index covers. An empty list means "all non-PK columns" — HCD will replicate
   * all non-primary-key columns automatically.
   */
  public List<CqlIdentifier> columns() {
    return columns;
  }

  /** The SAI index name as a string — convenience accessor for use in {@code expr()} predicates. */
  public String saiIndexName() {
    return indexName.asInternal();
  }

  // ── Schema description ───────────────────────────────────────────────────────

  @Override
  public IndexDesc<OpenSearchIndexDefinitionDesc> getSchemaDescription(
      SchemaDescSource schemaDescSource) {

    // Reconstruct the user-facing definition from the CQL options
    String openSearchIndexName = indexOptions.get("indexName");
    Integer numShards = intOption("numShards");
    Integer numReplicas = intOption("numReplicas");
    Boolean applyDefaultSchema =
        indexOptions.containsKey("applyDefaultSchema")
            ? Boolean.parseBoolean(indexOptions.get("applyDefaultSchema"))
            : null;

    // Reconstruct mappings from customMappingsJson if present
    JsonNode mappings = null;
    String customMappingsJson = indexOptions.get("customMappingsJson");
    if (customMappingsJson != null) {
      try {
        JsonNode wrapped = OBJECT_MAPPER.readTree(customMappingsJson);
        // The stored value is {"properties":{...}} — unwrap to expose only the user-visible part
        mappings = wrapped.path("properties");
        if (mappings.isMissingNode() || mappings.isNull()) {
          mappings = null;
        }
      } catch (JsonProcessingException e) {
        LOGGER.warn(
            "Failed to parse customMappingsJson for index {}: {}", indexName, e.getMessage());
      }
    }

    List<String> columnNames = columns.stream().map(CqlIdentifier::asInternal).toList();

    var options =
        new OpenSearchIndexDefinitionDesc.OpenSearchIndexOptions(
            numShards, numReplicas, openSearchIndexName, mappings, applyDefaultSchema);
    var definition =
        new OpenSearchIndexDefinitionDesc(columnNames.isEmpty() ? null : columnNames, options);

    final CqlIdentifier finalIndexName = indexName;
    return new IndexDesc<>() {
      @Override
      public String name() {
        return cqlIdentifierToJsonKey(finalIndexName);
      }

      @Override
      public String indexType() {
        return ApiIndexType.OPEN_SEARCH.apiName();
      }

      @Override
      public OpenSearchIndexDefinitionDesc definition() {
        return definition;
      }
    };
  }

  // ── Recordable ───────────────────────────────────────────────────────────────

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("indexName", indexName.asInternal())
        .append("indexType", ApiIndexType.OPEN_SEARCH)
        .append("columns", columns.stream().map(CqlIdentifier::asInternal).toList())
        .append("indexOptions", indexOptions);
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private Integer intOption(String key) {
    String v = indexOptions.get(key);
    return v == null ? null : Integer.valueOf(v);
  }

  // ── Inner factories ──────────────────────────────────────────────────────────

  /**
   * Creates an {@link ApiOpenSearchIndex} from a user-supplied {@link
   * OpenSearchIndexDefinitionDesc} (the {@code definition} field in {@code createOpenSearchIndex}).
   */
  private static class UserDescFactory
      extends IndexFactoryFromIndexDesc<ApiOpenSearchIndex, OpenSearchIndexDefinitionDesc> {

    @Override
    public ApiOpenSearchIndex create(
        TableSchemaObject tableSchemaObject,
        String indexName,
        OpenSearchIndexDefinitionDesc indexDesc) {

      Objects.requireNonNull(tableSchemaObject, "tableSchemaObject must not be null");
      Objects.requireNonNull(indexName, "indexName must not be null");

      var indexIdentifier = userNameToIdentifier(indexName, "indexName");

      // Validate columns exist if a list was provided
      List<CqlIdentifier> columnIds = List.of();
      if (indexDesc != null && indexDesc.columns() != null && !indexDesc.columns().isEmpty()) {
        columnIds =
            indexDesc.columns().stream()
                .map(
                    colName -> {
                      var id = userNameToIdentifier(colName, "column");
                      checkIndexColumnExists(tableSchemaObject, id);
                      return id;
                    })
                .toList();
      }

      // Build WITH OPTIONS map
      Map<String, String> opts = new HashMap<>();
      opts.put("createIndexIfNotExists", "true");

      OpenSearchIndexDefinitionDesc.OpenSearchIndexOptions options =
          indexDesc == null ? null : indexDesc.options();
      if (options != null) {
        if (options.numShards() != null) {
          opts.put("numShards", options.numShards().toString());
        }
        if (options.numReplicas() != null) {
          opts.put("numReplicas", options.numReplicas().toString());
        }
        if (options.openSearchIndexName() != null) {
          opts.put("indexName", options.openSearchIndexName());
        }
        if (options.mappings() != null && !options.mappings().isNull()) {
          // Wrap under {"properties":{...}} and serialise to JSON string
          try {
            var wrapped = OBJECT_MAPPER.createObjectNode();
            wrapped.set("properties", options.mappings());
            opts.put("customMappingsJson", OBJECT_MAPPER.writeValueAsString(wrapped));
            opts.put("applyCustomSchema", "true");
            opts.put("applyDefaultSchema", "false");
          } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialise OpenSearch mappings", e);
          }
        } else if (Boolean.FALSE.equals(options.applyDefaultSchema())) {
          opts.put("applyDefaultSchema", "false");
        }
      }

      return new ApiOpenSearchIndex(indexIdentifier, columnIds, opts);
    }
  }

  /**
   * Creates an {@link ApiOpenSearchIndex} from CQL {@link IndexMetadata} when the schema is read
   * back from Cassandra.
   */
  private static class CqlTypeFactory extends IndexFactoryFromCql {

    @Override
    protected ApiIndexDef create(
        ApiColumnDef apiColumnDef,
        CQLSAIIndex.IndexTarget indexTarget,
        IndexMetadata indexMetadata) {
      // OpenSearch indexes are multi-column and do not go through the single-column path.
      // This method should never be called for OPEN_SEARCH type; creation is done via
      // createFromMetadata().
      throw new IllegalStateException(
          "CqlTypeFactory.create(ApiColumnDef, IndexTarget, IndexMetadata) must not be called "
              + "for OpenSearch indexes. Use createFromMetadata(IndexMetadata) instead.");
    }

    /**
     * Creates an {@link ApiOpenSearchIndex} directly from {@link IndexMetadata} — bypassing the
     * single-column {@code IndexTarget} parsing that SAI indexes use.
     *
     * @param indexMetadata the Cassandra index metadata
     * @return the constructed index
     */
    public ApiOpenSearchIndex createFromMetadata(IndexMetadata indexMetadata) {
      Objects.requireNonNull(indexMetadata, "indexMetadata must not be null");
      // OpenSearch indexes target no specific column at the CQL level (they replicate all or a
      // user-specified subset); the column list is not stored in the Cassandra index metadata.
      return new ApiOpenSearchIndex(
          indexMetadata.getName(), List.of(), new HashMap<>(indexMetadata.getOptions()));
    }
  }
}
