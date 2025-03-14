package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionOnlyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.*;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that creates a collection.")
@JsonTypeName(CommandName.Names.CREATE_COLLECTION)
public record CreateCollectionCommand(
    @Schema(description = "Required name of the new Collection") String name,
    @Valid
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(
            description = "Configuration options for the collection",
            type = SchemaType.OBJECT,
            implementation = Options.class)
        Options options)
    implements CollectionOnlyCommand {
  public record Options(
      @Nullable
          @Valid
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @Schema(
              description = "Id configuration for the collection",
              type = SchemaType.OBJECT,
              implementation = VectorSearchConfig.class)
          @JsonProperty("defaultId")
          IdConfig idConfig,
      @Valid
          @Nullable
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @Schema(
              description = "Vector search configuration for the collection",
              type = SchemaType.OBJECT,
              implementation = VectorSearchConfig.class)
          VectorSearchConfig vector,
      @Valid
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @Nullable
          @Schema(
              description =
                  "Optional indexing configuration to provide allow/deny list of fields for indexing",
              type = SchemaType.OBJECT,
              implementation = IndexingConfig.class)
          IndexingConfig indexing,
      @Valid
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @Nullable
          @Schema(
              description =
                  "Optional configuration defining if and how to support use of '$lexical' field",
              type = SchemaType.OBJECT,
              implementation = LexicalConfigDefinition.class)
          LexicalConfigDefinition lexical,
      @Valid
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @Nullable
          @Schema(
              description = "Optional configuration defining if and how to support reranking",
              type = SchemaType.OBJECT,
              implementation = RerankingConfigDefinition.class)
          RerankingConfigDefinition reranking) {

    public record IdConfig(
        @Nullable
            @Pattern(
                regexp = "(objectId|uuid|uuidv6|uuidv7)",
                message = "Id type can only be 'objectId', 'uuid' , 'uuidv6' or 'uuidv7'")
            @Schema(
                description = "Id type for collection, default to 'uuid'",
                defaultValue = "uuid",
                type = SchemaType.STRING,
                implementation = String.class)
            @JsonProperty("type")
            String idType) {}

    public record VectorSearchConfig(
        @Nullable
            @Positive(message = "dimension should be greater than `0`")
            @Schema(
                description = "Dimension of the vector field",
                type = SchemaType.INTEGER,
                implementation = Integer.class)
            @JsonProperty("dimension")
            @JsonAlias("size") // old name
            Integer dimension,
        @Nullable
            @Pattern(
                regexp = "(dot_product|cosine|euclidean)",
                message = "function name can only be 'dot_product', 'cosine' or 'euclidean'")
            @Schema(
                description =
                    "Similarity function algorithm that needs to be used for vector search",
                defaultValue = "cosine",
                type = SchemaType.STRING,
                implementation = String.class)
            @JsonProperty("metric")
            @JsonAlias("function") // old name
            String metric,
        @Nullable
            @Pattern(
                regexp =
                    "(ada002|bert|cohere-v3|gecko|nv-qa-4|openai-v3-large|openai-v3-small|other)",
                message =
                    "sourceModel options are 'ada002', 'bert', 'cohere-v3', 'gecko', 'nv-qa-4', 'openai-v3-large', 'openai-v3-small', and 'other'")
            @Schema(
                description =
                    "The 'sourceModel' option configures the index with the fastest settings for a given source of embeddings vectors",
                defaultValue = "other",
                type = SchemaType.STRING,
                implementation = String.class)
            @JsonProperty("sourceModel")
            String sourceModel,
        @Valid
            @Nullable
            @JsonInclude(JsonInclude.Include.NON_NULL)
            @Schema(
                description = "Optional vectorize configuration to provide embedding service",
                type = SchemaType.OBJECT,
                implementation = VectorizeConfig.class)
            @JsonProperty("service")
            VectorizeConfig vectorizeConfig) {

      public VectorSearchConfig(
          Integer dimension, String metric, String sourceModel, VectorizeConfig vectorizeConfig) {
        this.dimension = dimension;
        this.metric = metric;
        this.sourceModel = sourceModel;
        this.vectorizeConfig = vectorizeConfig;
      }
    }

    public record IndexingConfig(
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
            @Schema(
                description = "List of allowed indexing fields",
                type = SchemaType.ARRAY,
                implementation = String.class)
            @Nullable
            List<String> allow,
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
            @Schema(
                description = "List of denied indexing fields",
                type = SchemaType.ARRAY,
                implementation = String.class)
            @Nullable
            List<String> deny) {

      public void validate() {
        if (allow() != null && deny() != null) {
          throw ErrorCodeV1.INVALID_INDEXING_DEFINITION.toApiException(
              "`allow` and `deny` cannot be used together");
        }

        if (allow() == null && deny() == null) {
          throw ErrorCodeV1.INVALID_INDEXING_DEFINITION.toApiException(
              "`allow` or `deny` should be provided");
        }

        if (allow() != null) {
          Set<String> dedupe = new HashSet<>(allow());
          if (dedupe.size() != allow().size()) {
            throw ErrorCodeV1.INVALID_INDEXING_DEFINITION.toApiException(
                "`allow` cannot contain duplicates");
          }
          validateIndexingPath(allow());
        }

        if (deny() != null) {
          Set<String> dedupe = new HashSet<>(deny());
          if (dedupe.size() != deny().size()) {
            throw ErrorCodeV1.INVALID_INDEXING_DEFINITION.toApiException(
                "`deny` cannot contain duplicates");
          }
          validateIndexingPath(deny());
        }
      }

      /**
       * Return `true if the indexing config for deny is set as "*"
       *
       * @return `true if the indexing config for deny is set as "*", `false` otherwise
       */
      public boolean denyAll() {
        return deny() != null && deny().contains("*");
      }

      /**
       * Validates the given paths name. A path name must not be empty and must not start with a $
       * except for $vector.
       *
       * @param paths the paths to validate
       */
      public void validateIndexingPath(List<String> paths) {
        // Special case: single "*" is accepted
        if (paths.size() == 1 && "*".equals(paths.get(0))) {
          return;
        }
        for (String path : paths) {
          if (!NamingRules.FIELD.apply(path)) {
            if (path.isEmpty()) {
              throw ErrorCodeV1.INVALID_INDEXING_DEFINITION.toApiException(
                  "path must be represented as a non-empty string");
            }
            if (path.startsWith("$")) {
              // $vector is allowed, otherwise throw error
              if (!DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(path)) {
                throw ErrorCodeV1.INVALID_INDEXING_DEFINITION.toApiException(
                    "path must not start with '$'");
              }
            }
          }

          try {
            DocumentPath.verifyEncodedPath(path);
          } catch (IllegalArgumentException e) {
            throw ErrorCodeV1.INVALID_INDEXING_DEFINITION.toApiException(
                "indexing path ('%s') is not a valid path. " + e.getMessage(), path);
          }
        }
      }
    }

    public record LexicalConfigDefinition(
        @Schema(
                description = "Whether to enable the use of '$lexical' field (default: 'true')",
                defaultValue = "true",
                type = SchemaType.BOOLEAN,
                implementation = Boolean.class,
                required = true)
            Boolean enabled,
        @Schema(
                description =
                    "Analyzer to use for '$lexical' field: either String (name of a pre-defined analyzer), or JSON Object to specify custom one. Default: 'standard')",
                defaultValue = "standard",
                oneOf = {String.class, Map.class})
            @JsonInclude(JsonInclude.Include.NON_NULL)
            @JsonProperty("analyzer")
            JsonNode analyzerDef) {}

    public record RerankingConfigDefinition(
        @Schema(
                description = "Whether to enable the use of reranking model (default: 'true')",
                defaultValue = "true",
                type = SchemaType.BOOLEAN,
                implementation = Boolean.class,
                required = true)
            Boolean enabled,
        @Schema(
                description =
                    "Reranking model configuration. Default is llama-3.2-nv-rerankqa-1b-v2 model from Nvidia.",
                defaultValue =
                    "\"service\": {\"provider\": \"nvidia\",\"modelName\": \"nvidia/llama-3.2-nv-rerankqa-1b-v2\"}",
                implementation = RerankingServiceConfig.class)
            @JsonInclude(JsonInclude.Include.NON_NULL)
            @JsonProperty("service")
            RerankingServiceConfig rerankingServiceConfig) {}

    public record RerankingServiceConfig(
        @NotNull
            @Schema(
                description = "Registered reranking service provider",
                type = SchemaType.STRING,
                implementation = String.class)
            @JsonProperty(VectorConstants.Vectorize.PROVIDER)
            String provider,
        @Schema(
                description = "Registered reranking service model",
                type = SchemaType.STRING,
                implementation = String.class)
            @JsonProperty(VectorConstants.Vectorize.MODEL_NAME)
            String modelName,
        @Valid
            @Nullable
            @Schema(
                description = "Authentication config for chosen reranking service",
                type = SchemaType.OBJECT)
            @JsonProperty(VectorConstants.Vectorize.AUTHENTICATION)
            @JsonInclude(JsonInclude.Include.NON_NULL)
            Map<String, String> authentication,
        @Nullable
            @Schema(
                description =
                    "Optional parameters that match the messageTemplate provided for the reranking provider",
                type = SchemaType.OBJECT)
            @JsonProperty(VectorConstants.Vectorize.PARAMETERS)
            @JsonInclude(JsonInclude.Include.NON_NULL)
            Map<String, Object> parameters) {}

    public Options(
        IdConfig idConfig,
        VectorSearchConfig vector,
        IndexingConfig indexing,
        LexicalConfigDefinition lexical,
        RerankingConfigDefinition reranking) {
      // idConfig could be null, will resolve idType to empty string in table comment
      this.idConfig = idConfig;
      this.vector = vector;
      this.indexing = indexing;
      this.lexical = lexical;
      this.reranking = reranking;
    }
  }

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.CREATE_COLLECTION;
  }
}
