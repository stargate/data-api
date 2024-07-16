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
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Refactored as seperate class that represent a collection property.*
 *
 * <p>TODO: there are a LOT of different ways this is constructed, need to refactor
 */
public final class CollectionSchemaObject extends SchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.COLLECTION;

  public static final CollectionSchemaObject MISSING =
      new CollectionSchemaObject(
          SchemaObjectName.MISSING,
          IdConfig.defaultIdConfig(),
          VectorConfig.notEnabledVectorConfig(),
          null);

  private final IdConfig idConfig;
  private final VectorConfig vectorConfig;
  private final IndexingConfig indexingConfig;

  /**
   * @param vectorConfig
   * @param indexingConfig
   */
  public CollectionSchemaObject(
      String keypaceName,
      String name,
      IdConfig idConfig,
      VectorConfig vectorConfig,
      IndexingConfig indexingConfig) {
    this(new SchemaObjectName(keypaceName, name), idConfig, vectorConfig, indexingConfig);
  }

  public CollectionSchemaObject(
      SchemaObjectName name,
      IdConfig idConfig,
      VectorConfig vectorConfig,
      IndexingConfig indexingConfig) {
    super(TYPE, name);
    this.idConfig = idConfig;
    this.vectorConfig = vectorConfig;
    this.indexingConfig = indexingConfig;
  }

  public CollectionSchemaObject withIdType(IdType idType) {
    return new CollectionSchemaObject(name, new IdConfig(idType), vectorConfig, indexingConfig);
  }

  @Override
  public VectorConfig vectorConfig() {
    return vectorConfig;
  }

  @Override
  public IndexUsage newIndexUsage() {
    return new CollectionIndexUsage();
  }

  /**
   * Helper to avoid cast from the interface method because there are times we need to set
   * properties on this immediately
   *
   * <p>Used in resolvers so they can set the vector tag for an ANN sort
   *
   * @return
   */
  public CollectionIndexUsage newCollectionIndexUsage() {
    return new CollectionIndexUsage();
  }

  public record IdConfig(IdType idType) {
    public static IdConfig defaultIdConfig() {
      return new IdConfig(IdType.UNDEFINED);
    }
  }

  public IndexingProjector indexingProjector() {
    // IndexingConfig null if no indexing definitions: default, index all:
    if (indexingConfig == null) {
      return IndexingProjector.identityProjector();
    }
    // otherwise get lazily initialized indexing projector from config
    return indexingConfig.indexingProjector();
  }

  public record IndexingConfig(
      Set<String> allowed, Set<String> denied, Supplier<IndexingProjector> indexedProject) {
    public IndexingConfig(Set<String> allowed, Set<String> denied) {
      this(
          allowed,
          denied,
          Suppliers.memoize(() -> IndexingProjector.createForIndexing(allowed, denied)));
    }

    public IndexingProjector indexingProjector() {
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
        default ->
            throw new JsonApiException(
                ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME,
                ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME.getMessage() + similarityFunction);
      };
    }
  }

  /** Collection Id Type enum, UNDEFINED represents unwrapped id */
  public enum IdType {
    OBJECT_ID,
    UUID,
    UUID_V6,
    UUID_V7,
    UNDEFINED;

    public static IdType fromString(String idType) {
      if (idType == null) return UNDEFINED;
      return switch (idType) {
        case "objectId" -> OBJECT_ID;
        case "uuid" -> UUID;
        case "uuidv6" -> UUID_V6;
        case "uuidv7" -> UUID_V7;
        case "" -> UNDEFINED;
        default -> throw ErrorCode.INVALID_ID_TYPE.toApiException(idType);
      };
    }

    public String toString() {
      return switch (this) {
        case OBJECT_ID -> "objectId";
        case UUID -> "uuid";
        case UUID_V6 -> "uuidv6";
        case UUID_V7 -> "uuidv7";
        case UNDEFINED -> "";
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
        default ->
            throw ErrorCode.VECTORIZE_INVALID_AUTHENTICATION_TYPE.toApiException(
                "'%s'", authenticationType);
      };
    }
  }

  public static CollectionSchemaObject getCollectionSettings(
      TableMetadata table, ObjectMapper objectMapper) {
    // [jsonapi#639]: get internal name to avoid quoting of case-sensitive names
    String keyspaceName = table.getKeyspace().asInternal();
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
      SimilarityFunction function = SimilarityFunction.COSINE;
      if (vectorIndex != null) {
        final String functionName =
            vectorIndex.getOptions().get(DocumentConstants.Fields.VECTOR_INDEX_FUNCTION_NAME);
        if (functionName != null) function = SimilarityFunction.fromString(functionName);
      }
      final String comment = (String) table.getOptions().get(CqlIdentifier.fromInternal("comment"));
      return createCollectionSettings(
          keyspaceName, collectionName, true, vectorSize, function, comment, objectMapper);
    } else { // if not vector collection
      // handling comment so get the indexing config from comment
      final String comment = (String) table.getOptions().get(CqlIdentifier.fromInternal("comment"));
      return createCollectionSettings(
          keyspaceName,
          collectionName,
          false,
          0,
          SimilarityFunction.UNDEFINED,
          comment,
          objectMapper);
    }
  }

  public static CollectionSchemaObject getCollectionSettings(
      String keyspaceName,
      String collectionName,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction similarityFunction,
      String comment,
      ObjectMapper objectMapper) {
    return createCollectionSettings(
        keyspaceName,
        collectionName,
        vectorEnabled,
        vectorSize,
        similarityFunction,
        comment,
        objectMapper);
  }

  private static CollectionSchemaObject createCollectionSettings(
      String keyspaceName,
      String collectionName,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction function,
      String comment,
      ObjectMapper objectMapper) {
    if (comment == null || comment.isBlank()) {
      if (vectorEnabled) {
        return new CollectionSchemaObject(
            keyspaceName,
            collectionName,
            IdConfig.defaultIdConfig(),
            new VectorConfig(true, vectorSize, function, null),
            null);
      } else {
        return new CollectionSchemaObject(
            keyspaceName,
            collectionName,
            IdConfig.defaultIdConfig(),
            VectorConfig.notEnabledVectorConfig(),
            null);
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
            return new CollectionSettingsV1Reader()
                .readCollectionSettings(collectionNode, keyspaceName, collectionName, objectMapper);
          default:
            throw ErrorCode.INVALID_SCHEMA_VERSION.toApiException();
        }
      } else {
        // backward compatibility for old indexing table comment
        // sample comment : {"indexing":{"deny":["address"]}}}
        return new CollectionSettingsV0Reader()
            .readCollectionSettings(
                commentConfigNode,
                keyspaceName,
                collectionName,
                vectorEnabled,
                vectorSize,
                function);
      }
    }
  }

  public static CreateCollectionCommand collectionSettingToCreateCollectionCommand(
      CollectionSchemaObject collectionSetting) {

    // TODO: move the vector and vectorize parts to be methods on those schema objects
    CreateCollectionCommand.Options options = null;
    CreateCollectionCommand.Options.VectorSearchConfig vectorSearchConfig = null;
    CreateCollectionCommand.Options.IndexingConfig indexingConfig = null;
    // populate the vectorSearchConfig
    if (collectionSetting.vectorConfig().vectorEnabled()) {
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig vectorizeConfig = null;
      if (collectionSetting.vectorConfig().vectorizeConfig() != null) {
        Map<String, String> authentication =
            collectionSetting.vectorConfig().vectorizeConfig().authentication();
        Map<String, Object> parameters =
            collectionSetting.vectorConfig().vectorizeConfig().parameters();
        vectorizeConfig =
            new CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig(
                collectionSetting.vectorConfig().vectorizeConfig().provider(),
                collectionSetting.vectorConfig().vectorizeConfig().modelName(),
                authentication == null ? null : Map.copyOf(authentication),
                parameters == null ? null : Map.copyOf(parameters));
      }
      vectorSearchConfig =
          new CreateCollectionCommand.Options.VectorSearchConfig(
              collectionSetting.vectorConfig().vectorSize(),
              collectionSetting.vectorConfig().similarityFunction().name().toLowerCase(),
              vectorizeConfig);
    }
    // populate the indexingConfig
    if (collectionSetting.indexingConfig() != null) {
      indexingConfig =
          new CreateCollectionCommand.Options.IndexingConfig(
              Lists.newArrayList(collectionSetting.indexingConfig().allowed()),
              Lists.newArrayList(collectionSetting.indexingConfig().denied()));
    }
    // construct the CreateCollectionCommand.options.idConfig -- but only if non-default IdType
    final IdType idType = collectionSetting.idConfig().idType();
    CreateCollectionCommand.Options.IdConfig idConfig =
        (idType == null || idType == IdType.UNDEFINED)
            ? null
            : new CreateCollectionCommand.Options.IdConfig(idType.toString());

    options = new CreateCollectionCommand.Options(idConfig, vectorSearchConfig, indexingConfig);

    // CreateCollectionCommand object is created for convenience to generate json
    // response. The code is not creating a collection here.
    return new CreateCollectionCommand(collectionSetting.name.table(), options);
  }

  public IdConfig idConfig() {
    return idConfig;
  }

  public IndexingConfig indexingConfig() {
    return indexingConfig;
  }

  // TODO: these helper functions break encapsulation for very little benefit
  public CollectionSchemaObject.SimilarityFunction similarityFunction() {
    return vectorConfig().similarityFunction();
  }

  public boolean isVectorEnabled() {
    return vectorConfig() != null && vectorConfig().vectorEnabled();
  }

  // TODO: the overrides below were auto added when migrating from a record to a class, not sure
  // they are needed or wanted
  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (CollectionSchemaObject) obj;
    return Objects.equals(this.name, that.name)
        && Objects.equals(this.idConfig, that.idConfig)
        && Objects.equals(this.vectorConfig, that.vectorConfig)
        && Objects.equals(this.indexingConfig, that.indexingConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, idConfig, vectorConfig, indexingConfig);
  }

  @Override
  public String toString() {
    return "CollectionSettings["
        + "name="
        + name
        + ", "
        + "idConfig="
        + idConfig
        + ", "
        + "vectorConfig="
        + vectorConfig
        + ", "
        + "indexingConfig="
        + indexingConfig
        + ']';
  }
}
