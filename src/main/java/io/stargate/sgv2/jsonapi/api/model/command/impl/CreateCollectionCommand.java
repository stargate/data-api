package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionOnlyCommand;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.*;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that creates a collection.")
@JsonTypeName("createCollection")
public record CreateCollectionCommand(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name of the collection")
        String name,
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
          IndexingConfig indexing) {

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
          String invalid = findInvalidPath(allow());
          if (invalid != null) {
            throw ErrorCodeV1.INVALID_INDEXING_DEFINITION.toApiException(
                "`allow` contains invalid path: '%s'", invalid);
          }
        }

        if (deny() != null) {
          Set<String> dedupe = new HashSet<>(deny());
          if (dedupe.size() != deny().size()) {
            throw ErrorCodeV1.INVALID_INDEXING_DEFINITION.toApiException(
                "`deny` cannot contain duplicates");
          }
          String invalid = findInvalidPath(deny());
          if (invalid != null) {
            throw ErrorCodeV1.INVALID_INDEXING_DEFINITION.toApiException(
                "`deny` contains invalid path: '%s'", invalid);
          }
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

      public String findInvalidPath(List<String> paths) {
        // Special case: single "*" is accepted
        if (paths.size() == 1 && "*".equals(paths.get(0))) {
          return null;
        }
        for (String path : paths) {
          if (!DocumentConstants.Fields.VALID_PATH_PATTERN.matcher(path).matches()) {
            // One exception: $vector is allowed
            if (!DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(path)) {
              return path;
            }
          }
        }
        return null;
      }
    }

    public Options(IdConfig idConfig, VectorSearchConfig vector, IndexingConfig indexing) {
      // idConfig could be null, will resolve idType to empty string in table comment
      this.idConfig = idConfig;
      this.vector = vector;
      this.indexing = indexing;
    }
  }

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.CREATE_COLLECTION;
  }
}
