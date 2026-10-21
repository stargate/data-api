package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.Nullable;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Definition descriptor for the {@code createOpenSearchIndex} command.
 *
 * <p>The {@code columns} field is the list of table columns to replicate; when absent HCD
 * replicates all non-primary-key columns automatically. The {@code options} block carries the
 * OpenSearch-side settings (index name, shard count, replica count, field mappings).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OpenSearchIndexDefinitionDesc(
    @Nullable
        @Schema(
            description =
                "Optional list of table column names to replicate to OpenSearch."
                    + " When omitted, all non-primary-key columns are replicated.")
        @JsonProperty("columns")
        List<String> columns,
    @Nullable
        @Schema(description = "Options for the OpenSearch index.", type = SchemaType.OBJECT)
        @JsonProperty("options")
        OpenSearchIndexOptions options)
    implements IndexDefinitionDesc<
        List<String>, OpenSearchIndexDefinitionDesc.OpenSearchIndexOptions> {

  /** {@inheritDoc} — returns the columns list (plural) from this multi-column index descriptor. */
  @Override
  public List<String> column() {
    return columns;
  }

  /** Options that control OpenSearch-side settings for the index. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record OpenSearchIndexOptions(
      @Nullable
          @Schema(description = "Number of primary shards for the OpenSearch index.")
          @JsonProperty("numShards")
          Integer numShards,
      @Nullable
          @Schema(description = "Number of replicas for the OpenSearch index.")
          @JsonProperty("numReplicas")
          Integer numReplicas,
      /**
       * OS index name — the OpenSearch index name (passed as {@code 'indexName'} in {@code WITH
       * OPTIONS}). Defaults to {@code hcd_<keyspace>_<tableName>} when absent.
       */
      @Nullable
          @Schema(
              description =
                  "OpenSearch index name. Defaults to hcd_<keyspace>_<tableName> when absent.")
          @JsonProperty("openSearchIndexName")
          String openSearchIndexName,
      /**
       * Flat field-name → OpenSearch field definition map. When present the Data API wraps it under
       * {@code {"properties":{...}}} and sets {@code applyCustomSchema: true}.
       */
      @Nullable
          @Schema(
              description =
                  "Optional flat field-name to OpenSearch field definition map."
                      + " Provide only when the default HCD schema inference is insufficient.",
              type = SchemaType.OBJECT)
          @JsonProperty("mappings")
          JsonNode mappings,
      /**
       * When {@code false}, HCD will not derive the field mapping from Cassandra column types —
       * useful when relying on an existing OpenSearch index template. Defaults to {@code true}.
       */
      @Nullable
          @Schema(
              description =
                  "When false, HCD ignores the Cassandra schema for mapping inference."
                      + " Defaults to true.",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          @JsonProperty("applyDefaultSchema")
          Boolean applyDefaultSchema) {}
}
