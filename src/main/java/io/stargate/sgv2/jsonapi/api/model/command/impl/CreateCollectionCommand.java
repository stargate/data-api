package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.NamespaceCommand;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    implements NamespaceCommand {
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
        @Valid
            @Nullable
            @JsonInclude(JsonInclude.Include.NON_NULL)
            @Schema(
                description = "Optional vectorize configuration to provide embedding service",
                type = SchemaType.OBJECT,
                implementation = VectorizeConfig.class)
            @JsonProperty("service")
            VectorizeConfig vectorizeConfig) {

      public VectorSearchConfig(Integer dimension, String metric, VectorizeConfig vectorizeConfig) {
        this.dimension = dimension;
        this.metric = metric == null ? "cosine" : metric;
        this.vectorizeConfig = vectorizeConfig;
      }

      public record VectorizeConfig(
          @NotNull
              @Schema(
                  description = "Registered Embedding service provider",
                  type = SchemaType.STRING,
                  implementation = String.class)
              @JsonProperty("provider")
              String provider,
          @NotNull
              @Schema(
                  description = "Registered Embedding service model",
                  type = SchemaType.STRING,
                  implementation = String.class)
              @JsonProperty("modelName")
              String modelName,
          @Valid
              @Nullable
              @Schema(
                  description = "Authentication config for chosen embedding service",
                  type = SchemaType.OBJECT,
                  implementation = VectorizeServiceAuthentication.class)
              @JsonProperty("authentication")
              @JsonInclude(JsonInclude.Include.NON_NULL)
              VectorizeServiceAuthentication vectorizeServiceAuthentication,
          @Nullable
              @Schema(
                  description =
                      "Optional parameters that match the template provided for the provider",
                  type = SchemaType.OBJECT)
              @JsonProperty("parameters")
              @JsonInclude(JsonInclude.Include.NON_NULL)
              Map<String, Object> vectorizeServiceParameter) {
        public record VectorizeServiceAuthentication(
            @Nullable
                @JsonInclude(JsonInclude.Include.NON_NULL)
                @Schema(
                    description =
                        "List of authentications that can be used when sending documents that need vectorization. One or more of \"NONE\", \"HEADER\", \"SHARED_SECRET\"",
                    type = SchemaType.ARRAY,
                    implementation = String.class)
                @JsonProperty("type")
                List<
                        @Pattern(
                            regexp = "(NONE|HEADER|SHARED_SECRET)",
                            message =
                                "authentication type can only be one or more of 'NONE', 'HEADER' or 'SHARED_SECRET'")
                        String>
                    type,
            @Nullable
                @JsonInclude(JsonInclude.Include.NON_NULL)
                @Schema(
                    description =
                        "Secret name. when stored_secrets authentication is used must be provided with the name of a pre-registered secret",
                    type = SchemaType.STRING,
                    implementation = String.class)
                @JsonProperty("secretName")
                String secretName) {}
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
          throw ErrorCode.INVALID_INDEXING_DEFINITION.toApiException(
              "`allow` and `deny` cannot be used together");
        }

        if (allow() == null && deny() == null) {
          throw ErrorCode.INVALID_INDEXING_DEFINITION.toApiException(
              "`allow` or `deny` should be provided");
        }

        if (allow() != null) {
          Set<String> dedupe = new HashSet<>(allow());
          if (dedupe.size() != allow().size()) {
            throw ErrorCode.INVALID_INDEXING_DEFINITION.toApiException(
                "`allow` cannot contain duplicates");
          }
          String invalid = findInvalidPath(allow());
          if (invalid != null) {
            throw ErrorCode.INVALID_INDEXING_DEFINITION.toApiException(
                "`allow` contains invalid path: '%s'", invalid);
          }
        }

        if (deny() != null) {
          Set<String> dedupe = new HashSet<>(deny());
          if (dedupe.size() != deny().size()) {
            throw ErrorCode.INVALID_INDEXING_DEFINITION.toApiException(
                "`deny` cannot contain duplicates");
          }
          String invalid = findInvalidPath(deny());
          if (invalid != null) {
            throw ErrorCode.INVALID_INDEXING_DEFINITION.toApiException(
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
}
