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
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
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
 * Refactored as seperate class that represent a collection property.*
 *
 * @param collectionName
 * @param vectorConfig
 * @param indexingConfig
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

  /**
   * incorporates vectorizeConfig into vectorConfig
   *
   * @param vectorEnabled
   * @param vectorSize
   * @param similarityFunction
   * @param vectorizeConfig
   */
  public record VectorConfig(
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction similarityFunction,
      VectorizeConfig vectorizeConfig) {

    public static VectorConfig notEnabledVectorConfig() {
      return new VectorConfig(false, -1, null, null);
    }

    // convert a vector jsonNode from table comment to vectorConfig
    public static VectorConfig fromJson(JsonNode jsonNode, ObjectMapper objectMapper) {
      // dimension, similarityFunction, must exist
      int dimension = jsonNode.get("dimension").asInt();
      SimilarityFunction similarityFunction =
          SimilarityFunction.fromString(jsonNode.get("metric").asText());

      VectorizeConfig vectorizeConfig = null;
      // construct vectorizeConfig
      JsonNode vectorizeServiceNode = jsonNode.get("service");
      if (vectorizeServiceNode != null) {
        // provider, model_name, must exist
        String provider = vectorizeServiceNode.get("provider").asText();
        String modelName = vectorizeServiceNode.get("model_name").asText();
        // construct VectorizeConfig.VectorizeServiceAuthentication, can be null
        JsonNode vectorizeServiceAuthenticationNode = vectorizeServiceNode.get("authentication");
        VectorizeConfig.VectorizeServiceAuthentication vectorizeServiceAuthentication = null;
        if (vectorizeServiceAuthenticationNode != null) {
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
          vectorizeServiceAuthentication =
              new VectorizeConfig.VectorizeServiceAuthentication(
                  authenticationTypeSet, authenticationSecretName);
        }
        // construct VectorizeConfig.VectorizeServiceParameter, can be null
        JsonNode vectorizeServiceParameterNode = vectorizeServiceNode.get("parameters");
        Map<String, Object> vectorizeServiceParameter =
            vectorizeServiceParameterNode == null
                ? null
                : objectMapper.convertValue(vectorizeServiceParameterNode, Map.class);
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
        Map<String, Object> vectorizeServiceParameter) {

      public record VectorizeServiceAuthentication(
          Set<AuthenticationType> type, String secretName) {}
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
    // if vector column exists
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
      if (vectorEnabled) {
        return new CollectionSettings(
            collectionName, new VectorConfig(true, vectorSize, function, null), null);
      } else {
        return new CollectionSettings(collectionName, VectorConfig.notEnabledVectorConfig(), null);
      }
    } else {
      JsonNode commentConfigNode;
      try {
        commentConfigNode = objectMapper.readTree(comment);
      } catch (JsonProcessingException e) {
        // This should never happen, already check if vectorize is a valid JSON
        throw new RuntimeException("Invalid json string, please check 'options' configuration.", e);
      }
      // new table comment design from schema_version v1, with collection as top-level key
      JsonNode collectionNode = commentConfigNode.get(TableCommentConstants.TOP_LEVEL_KEY);
      if (collectionNode != null) {
        final JsonNode schemaVersionNode =
            collectionNode.get(TableCommentConstants.SCHEMA_VERSION_KEY);
        if (schemaVersionNode == null) {
          throw ErrorCode.INVALID_SCHEMA_VERSION.toApiException();
        }
        switch (collectionNode.get(TableCommentConstants.SCHEMA_VERSION_KEY).asInt()) {
          case 1:
            return new CommandSettingsV1Deserializer()
                .deserialize(collectionNode, collectionName, objectMapper);
          default:
            throw ErrorCode.INVALID_SCHEMA_VERSION.toApiException();
        }
      } else {
        // backward compatibility for old indexing table comment
        // sample comment : {"indexing":{"deny":["address"]}}}
        return new CommandSettingsV0Deserializer()
            .deserialize(commentConfigNode, collectionName, vectorEnabled, vectorSize, function);
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
      if (collectionSetting.vectorConfig.vectorizeConfig != null) {
        CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig
                .VectorizeServiceAuthentication
            vectorizeServiceAuthentication = null;
        if (collectionSetting.vectorConfig.vectorizeConfig.vectorizeServiceAuthentication != null) {
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
        }
        Map<String, Object> vectorizeServiceParameter =
            collectionSetting.vectorConfig.vectorizeConfig.vectorizeServiceParameter;
        vectorizeConfig =
            new CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig(
                collectionSetting.vectorConfig.vectorizeConfig.provider,
                collectionSetting.vectorConfig.vectorizeConfig.modelName,
                vectorizeServiceAuthentication,
                vectorizeServiceParameter == null ? null : Map.copyOf(vectorizeServiceParameter));
      }
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
