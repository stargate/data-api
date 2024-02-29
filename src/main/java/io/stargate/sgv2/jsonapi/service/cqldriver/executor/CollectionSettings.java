package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Refactored as seperate class that represent a collection property.
 *
 * @param collectionName
 * @param vectorEnabled
 * @param vectorSize
 * @param similarityFunction #TODO
 */
public record CollectionSettings(
    String collectionName, VectorConfig vectorConfig, IndexingConfig indexingConfig) {

  private static final CollectionSettings EMPTY =
      new CollectionSettings("", VectorConfig.notEnabledVectorConfig(), null);

  public static CollectionSettings empty() {
    return EMPTY;
  }

  public DocumentProjector indexingProjector() {
    // IndexingConfig null if no indexing definitions: default, index all:
    if (indexingConfig == null) {
      return DocumentProjector.identityProjector();
    }
    // otherwise get lazily initialized indexing projector from config
    return indexingConfig.indexingProjector();
  }

  public record IndexingConfig(
      Set<String> allowed, Set<String> denied, Supplier<DocumentProjector> indexedProject) {
    public IndexingConfig(Set<String> allowed, Set<String> denied) {
      this(
          allowed,
          denied,
          Suppliers.memoize(() -> DocumentProjector.createForIndexing(allowed, denied)));
    }

    public DocumentProjector indexingProjector() {
      return indexedProject.get();
    }

    public static IndexingConfig fromJson(JsonNode jsonNode) {
      Set<String> allowed = new HashSet<>();
      Set<String> denied = new HashSet<>();
      if (jsonNode.has("allow")) {
        jsonNode.get("allow").forEach(node -> allowed.add(node.asText()));
      }
      if (jsonNode.has("deny")) {
        jsonNode.get("deny").forEach(node -> denied.add(node.asText()));
      }
      return new IndexingConfig(allowed, denied);
    }

    // Need to override to prevent comparison of the supplier
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o instanceof IndexingConfig other) {
        return Objects.equals(this.allowed, other.allowed)
            && Objects.equals(this.denied, other.denied);
      }
      return false;
    }
  }

  public record VectorConfig(
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction similarityFunction,
      VectorizeConfig vectorizeConfig) {

    public static VectorConfig notEnabledVectorConfig() {
      return new VectorConfig(false, -1, null, null);
    }

    public static VectorConfig fromJson(JsonNode jsonNode) {
      // dimension, similarityFunction, must exist
      int dimension = jsonNode.get("dimension").asInt();
      SimilarityFunction similarityFunction =
          SimilarityFunction.fromString(jsonNode.get("metric").asText());

      VectorizeConfig vectorizeConfig = null;
      // construct vectorizeConfig
      if (jsonNode.has("service")) {
        JsonNode vectorizeServiceNode = jsonNode.get("service");
        // provider, model_name, authentication, must exist
        String provider = vectorizeServiceNode.get("provider").asText();
        String modelName = vectorizeServiceNode.get("model_name").asText();
        // construct VectorizeConfig.VectorizeServiceAuthentication
        JsonNode vectorizeServiceAuthenticationNode = vectorizeServiceNode.get("authentication");
        Set<AuthenticationType> authenticationTypeSet = new HashSet<>();
        vectorizeServiceAuthenticationNode
            .get("type")
            .forEach(
                node -> authenticationTypeSet.add(AuthenticationType.fromString(node.asText())));
        // when stored_secrets authentication is used must be provided with the name of a
        // pre-registered secret.
        String authenticationSecretName =
            vectorizeServiceAuthenticationNode.has("secret_name")
                ? vectorizeServiceAuthenticationNode.get("secret_name").asText()
                : null;
        VectorizeConfig.VectorizeServiceAuthentication vectorizeServiceAuthentication =
            new VectorizeConfig.VectorizeServiceAuthentication(
                authenticationTypeSet, authenticationSecretName);

        // construct VectorizeConfig.VectorizeServiceParameter
        JsonNode VectorizeServiceParameterNode = vectorizeServiceNode.get("parameters");
        String projectId =
            VectorizeServiceParameterNode.has("project_id")
                ? VectorizeServiceParameterNode.get("project_id").asText()
                : null;
        VectorizeConfig.VectorizeServiceParameter vectorizeServiceParameter =
            new VectorizeConfig.VectorizeServiceParameter(projectId);

        vectorizeConfig =
            new VectorizeConfig(
                provider, modelName, vectorizeServiceAuthentication, vectorizeServiceParameter);
      }

      return new VectorConfig(true, dimension, similarityFunction, vectorizeConfig);
    }

    public record VectorizeConfig(
        String provider,
        String modelName,
        VectorizeServiceAuthentication vectorizeServiceAuthentication,
        VectorizeServiceParameter vectorizeServiceParameter) {

      public record VectorizeServiceAuthentication(
          Set<AuthenticationType> type, String secretName) {}

      public record VectorizeServiceParameter(String projectId) {}
    }
  }

  /**
   * The similarity function used for the vector index. This is only applicable if the vector index
   * is enabled.
   */
  public enum SimilarityFunction {
    COSINE,
    EUCLIDEAN,
    DOT_PRODUCT,
    UNDEFINED;

    public static SimilarityFunction fromString(String similarityFunction) {
      if (similarityFunction == null) return UNDEFINED;
      return switch (similarityFunction.toLowerCase()) {
        case "cosine" -> COSINE;
        case "euclidean" -> EUCLIDEAN;
        case "dot_product" -> DOT_PRODUCT;
        default -> throw new JsonApiException(
            ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME,
            ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME.getMessage() + similarityFunction);
      };
    }
  }

  public enum AuthenticationType {
    NONE,
    HEADER,
    SHARED_SECRET,
    UNDEFINED;

    public static AuthenticationType fromString(String authenticationType) {
      if (authenticationType == null) return UNDEFINED;
      return switch (authenticationType.toLowerCase()) {
        case "none" -> NONE;
        case "header" -> HEADER;
        case "shared_secret" -> SHARED_SECRET;
        default -> throw ErrorCode.VECTORIZE_INVALID_AUTHENTICATION_TYPE.toApiException(
            "'%s'", authenticationType);
      };
    }
  }

  public static CollectionSettings getCollectionSettings(
      TableMetadata table, ObjectMapper objectMapper) {
    // [jsonapi#639]: get internal name to avoid quoting of case-sensitive names
    String collectionName = table.getName().asInternal();
    // get vector column
    final Optional<ColumnMetadata> vectorColumn =
        table.getColumn(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME);
    boolean vectorEnabled = vectorColumn.isPresent();
    // if vector column exist
    if (vectorEnabled) {
      final int vectorSize = ((VectorType) vectorColumn.get().getType()).getDimensions();
      // get vector index
      IndexMetadata vectorIndex = null;
      Map<CqlIdentifier, IndexMetadata> indexMap = table.getIndexes();
      for (CqlIdentifier key : indexMap.keySet()) {
        if (key.asInternal().endsWith(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME)) {
          vectorIndex = indexMap.get(key);
          break;
        }
      }
      // default function
      CollectionSettings.SimilarityFunction function = CollectionSettings.SimilarityFunction.COSINE;
      if (vectorIndex != null) {
        final String functionName =
            vectorIndex.getOptions().get(DocumentConstants.Fields.VECTOR_INDEX_FUNCTION_NAME);
        if (functionName != null)
          function = CollectionSettings.SimilarityFunction.fromString(functionName);
      }
      final String comment = (String) table.getOptions().get(CqlIdentifier.fromInternal("comment"));
      return createCollectionSettings(
          collectionName, true, vectorSize, function, comment, objectMapper);
    } else { // if not vector collection
      // handling comment so get the indexing config from comment
      final String comment = (String) table.getOptions().get(CqlIdentifier.fromInternal("comment"));
      return createCollectionSettings(
          collectionName,
          false,
          0,
          CollectionSettings.SimilarityFunction.UNDEFINED,
          comment,
          objectMapper);
    }
  }

  public static CollectionSettings getCollectionSettings(
      String collectionName,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction similarityFunction,
      String comment,
      ObjectMapper objectMapper) {
    return createCollectionSettings(
        collectionName, vectorEnabled, vectorSize, similarityFunction, comment, objectMapper);
  }

  private static CollectionSettings createCollectionSettings(
      String collectionName,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction function,
      String comment,
      ObjectMapper objectMapper) {
    if (comment == null || comment.isBlank()) {
      // vector column exists -> vectorEnabled, vectorSize
      // vector index exists -> similarityFunction
      if (vectorEnabled) {
        return new CollectionSettings(
            collectionName, new VectorConfig(true, vectorSize, function, null), null);
      } else {
        return new CollectionSettings(collectionName, VectorConfig.notEnabledVectorConfig(), null);
      }
    } else {
      JsonNode commentConfig;
      try {
        commentConfig = objectMapper.readTree(comment);
      } catch (JsonProcessingException e) {
        // This should never happen, already check if vectorize is a valid JSON
        throw new RuntimeException("Invalid json string, please check 'options' configuration.", e);
      }

      // new table comment design, with collectionOptions as top-level key
      if (commentConfig.has("collectionOptions")) {
        JsonNode collectionOptionsNode = commentConfig.get("collectionOptions");
        VectorConfig vectorConfig = null;
        JsonNode vector = collectionOptionsNode.path("vector");
        if (!vector.isMissingNode()) {
          vectorConfig = VectorConfig.fromJson(vector);
        }
        IndexingConfig indexingConfig = null;
        JsonNode indexing = collectionOptionsNode.path("indexing");
        if (!indexing.isMissingNode()) {
          indexingConfig = IndexingConfig.fromJson(indexing);
        }
        return new CollectionSettings(collectionName, vectorConfig, indexingConfig);
      } else {
        // backward compatibility for old vectorize table comment
        VectorConfig.VectorizeConfig vectorizeConfig = null;
        JsonNode vectorize = commentConfig.path("vectorize");
        if (!vectorize.isMissingNode()) {
          String service = vectorize.get("service").asText();
          String modelName = null;
          JsonNode vectorizeOptions = vectorize.path("options");
          if (!vectorizeOptions.isMissingNode()) {
            modelName = vectorizeOptions.get("modelName").asText();
          }
          vectorizeConfig =
              new VectorConfig.VectorizeConfig(
                  service,
                  modelName,
                  new VectorConfig.VectorizeConfig.VectorizeServiceAuthentication(
                      Set.of(AuthenticationType.HEADER), null),
                  null);
        }
        VectorConfig vectorConfig =
            new VectorConfig(vectorEnabled, vectorSize, function, vectorizeConfig);

        // backward compatibility for old indexing table comment
        IndexingConfig indexingConfig = null;
        JsonNode indexing = commentConfig.path("indexing");
        if (!indexing.isMissingNode()) {
          indexingConfig = IndexingConfig.fromJson(indexing);
        }
        return new CollectionSettings(collectionName, vectorConfig, indexingConfig);
      }
    }
  }

  public static CreateCollectionCommand collectionSettingToCreateCollectionCommand(
      CollectionSettings collectionSetting) {
    CreateCollectionCommand.Options options = null;
    CreateCollectionCommand.Options.VectorSearchConfig vectorSearchConfig = null;
    CreateCollectionCommand.Options.IndexingConfig indexingConfig = null;
    // populate the vectorSearchConfig
    if (collectionSetting.vectorConfig.vectorEnabled) {
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig vectorizeConfig = null;
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig
              .VectorizeServiceAuthentication
          vectorizeServiceAuthentication =
              new CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig
                  .VectorizeServiceAuthentication(
                  collectionSetting
                      .vectorConfig
                      .vectorizeConfig
                      .vectorizeServiceAuthentication
                      .type
                      .stream()
                      .map(Enum::name)
                      .collect(Collectors.toList()),
                  collectionSetting
                      .vectorConfig
                      .vectorizeConfig
                      .vectorizeServiceAuthentication
                      .secretName);
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig.VectorizeServiceParameter
          vectorizeServiceParameter =
              new CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig
                  .VectorizeServiceParameter(
                  collectionSetting
                      .vectorConfig
                      .vectorizeConfig
                      .vectorizeServiceParameter
                      .projectId);
      vectorizeConfig =
          new CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig(
              collectionSetting.vectorConfig.vectorizeConfig.provider,
              collectionSetting.vectorConfig.vectorizeConfig.modelName,
              vectorizeServiceAuthentication,
              vectorizeServiceParameter);
      vectorSearchConfig =
          new CreateCollectionCommand.Options.VectorSearchConfig(
              collectionSetting.vectorConfig.vectorSize,
              collectionSetting.vectorConfig.similarityFunction.name().toLowerCase(),
              vectorizeConfig);
    }
    // populate the indexingConfig
    if (collectionSetting.indexingConfig() != null) {
      indexingConfig =
          new CreateCollectionCommand.Options.IndexingConfig(
              Lists.newArrayList(collectionSetting.indexingConfig().allowed()),
              Lists.newArrayList(collectionSetting.indexingConfig().denied()));
    }
    if (vectorSearchConfig != null || indexingConfig != null) {
      options = new CreateCollectionCommand.Options(vectorSearchConfig, indexingConfig);
    }

    // CreateCollectionCommand object is created for convenience to generate json
    // response. The code is not creating a collection here.
    return new CreateCollectionCommand(collectionSetting.collectionName(), options);
  }
}
