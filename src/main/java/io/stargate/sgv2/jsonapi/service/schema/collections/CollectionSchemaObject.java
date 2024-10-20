package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Refactored as seperate class that represent a collection property.*
 *
 * <p>TODO: there are a LOT of different ways this is constructed, need to refactor
 */
public final class CollectionSchemaObject extends TableBasedSchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.COLLECTION;

  public static final CollectionSchemaObject MISSING =
      new CollectionSchemaObject(
          SchemaObjectName.MISSING,
          null,
          IdConfig.defaultIdConfig(),
          VectorConfig.NOT_ENABLED_CONFIG,
          null);

  private final IdConfig idConfig;
  private final VectorConfig vectorConfig;
  private final CollectionIndexingConfig indexingConfig;
  private final TableMetadata tableMetadata;

  /**
   * @param vectorConfig
   * @param indexingConfig
   */
  public CollectionSchemaObject(
      String keypaceName,
      String name,
      TableMetadata tableMetadata,
      IdConfig idConfig,
      VectorConfig vectorConfig,
      CollectionIndexingConfig indexingConfig) {
    this(
        new SchemaObjectName(keypaceName, name),
        tableMetadata,
        idConfig,
        vectorConfig,
        indexingConfig);
  }

  public CollectionSchemaObject(
      SchemaObjectName name,
      TableMetadata tableMetadata,
      IdConfig idConfig,
      VectorConfig vectorConfig,
      CollectionIndexingConfig indexingConfig) {
    super(TYPE, name, tableMetadata);

    this.idConfig = idConfig;
    this.vectorConfig = vectorConfig;
    this.indexingConfig = indexingConfig;
    this.tableMetadata = tableMetadata;
  }

  // TODO: remove this, it is just here for testing and can be handled by creating test data
  // effectively
  public CollectionSchemaObject withIdType(CollectionIdType idType) {
    return new CollectionSchemaObject(
        name(), tableMetadata, new IdConfig(idType), vectorConfig, indexingConfig);
  }

  @Override
  public VectorConfig vectorConfig() {
    return vectorConfig;
  }

  @Override
  public IndexUsage newIndexUsage() {
    return newCollectionIndexUsage();
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

  public IndexingProjector indexingProjector() {
    // IndexingConfig null if no indexing definitions: default, index all:
    if (indexingConfig == null) {
      return IndexingProjector.identityProjector();
    }
    // otherwise get lazily initialized indexing projector from config
    return indexingConfig.indexingProjector();
  }

  // TODO: AARON COMMENTED OUT TO SEE IF IT IS USED
  //  public enum AuthenticationType {
  //    NONE,
  //    HEADER,
  //    SHARED_SECRET,
  //    UNDEFINED;
  //
  //    public static AuthenticationType fromString(String authenticationType) {
  //      if (authenticationType == null) return UNDEFINED;
  //      return switch (authenticationType.toLowerCase()) {
  //        case "none" -> NONE;
  //        case "header" -> HEADER;
  //        case "shared_secret" -> SHARED_SECRET;
  //        default ->
  //            throw ErrorCodeV1.VECTORIZE_INVALID_AUTHENTICATION_TYPE.toApiException(
  //                "'%s'", authenticationType);
  //      };
  //    }
  //  }

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
        if (functionName != null) {
          function = SimilarityFunction.fromString(functionName);
        }
      }
      final String comment = (String) table.getOptions().get(CqlIdentifier.fromInternal("comment"));
      return createCollectionSettings(
          keyspaceName, collectionName, table, true, vectorSize, function, comment, objectMapper);
    } else { // if not vector collection
      // handling comment so get the indexing config from comment
      final String comment = (String) table.getOptions().get(CqlIdentifier.fromInternal("comment"));
      return createCollectionSettings(
          keyspaceName,
          collectionName,
          table,
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
      TableMetadata tableMetadata,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction similarityFunction,
      String comment,
      ObjectMapper objectMapper) {
    return createCollectionSettings(
        keyspaceName,
        collectionName,
        tableMetadata,
        vectorEnabled,
        vectorSize,
        similarityFunction,
        comment,
        objectMapper);
  }

  private static CollectionSchemaObject createCollectionSettings(
      String keyspaceName,
      String collectionName,
      TableMetadata tableMetadata,
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
            tableMetadata,
            IdConfig.defaultIdConfig(),
            VectorConfig.fromColumnDefinitions(
                List.of(
                    new VectorConfig.ColumnVectorDefinition(
                        DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                        vectorSize,
                        function,
                        null))),
            null);
      } else {
        return new CollectionSchemaObject(
            keyspaceName,
            collectionName,
            tableMetadata,
            IdConfig.defaultIdConfig(),
            VectorConfig.NOT_ENABLED_CONFIG,
            null);
      }
    } else {
      JsonNode commentConfigNode;
      try {
        commentConfigNode = objectMapper.readTree(comment);
      } catch (JacksonException e) {
        // This should never happen, already check if vectorize is a valid JSON
        throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
            e, "Invalid JSON in Table comment for Collection, problem: %s", e.getMessage());
      }
      // new table comment design from schema_version v1, with collection as top-level key
      JsonNode collectionNode = commentConfigNode.get(TableCommentConstants.TOP_LEVEL_KEY);
      if (collectionNode != null) {
        final JsonNode schemaVersionNode =
            collectionNode.get(TableCommentConstants.SCHEMA_VERSION_KEY);
        if (schemaVersionNode == null) {
          throw ErrorCodeV1.INVALID_SCHEMA_VERSION.toApiException();
        }
        switch (collectionNode.get(TableCommentConstants.SCHEMA_VERSION_KEY).asInt()) {
          case 1:
            return new CollectionSettingsV1Reader()
                .readCollectionSettings(
                    collectionNode, keyspaceName, collectionName, tableMetadata, objectMapper);
          default:
            throw ErrorCodeV1.INVALID_SCHEMA_VERSION.toApiException();
        }
      } else {
        // backward compatibility for old indexing table comment
        // sample comment : {"indexing":{"deny":["address"]}}}
        return new CollectionSettingsV0Reader()
            .readCollectionSettings(
                commentConfigNode,
                keyspaceName,
                collectionName,
                tableMetadata,
                vectorEnabled,
                vectorSize,
                function);
      }
    }
  }

  // convert a vector jsonNode from cql table comment to vectorConfig, used for collection
  private static VectorConfig.ColumnVectorDefinition fromJson(
      JsonNode jsonNode, ObjectMapper objectMapper) {
    // dimension, similarityFunction, must exist
    int dimension = jsonNode.get("dimension").asInt();
    SimilarityFunction similarityFunction =
        SimilarityFunction.fromString(jsonNode.get("metric").asText());

    return VectorConfig.ColumnVectorDefinition.fromJson(
        DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
        dimension,
        similarityFunction,
        jsonNode,
        objectMapper);
  }

  public static CreateCollectionCommand collectionSettingToCreateCollectionCommand(
      CollectionSchemaObject collectionSetting) {

    // TODO: move the vector and vectorize parts to be methods on those schema objects
    CreateCollectionCommand.Options options = null;
    CreateCollectionCommand.Options.VectorSearchConfig vectorSearchConfig = null;
    CreateCollectionCommand.Options.IndexingConfig indexingConfig = null;
    // populate the vectorSearchConfig, Default will be the index 0 since there is only one vector
    // column supported for collection
    final VectorConfig vectorConfig = collectionSetting.vectorConfig();
    if (vectorConfig.vectorEnabled()) {
      // This will be size 1 for collection
      VectorConfig.ColumnVectorDefinition vectorConfigColumn =
          vectorConfig.columnVectorDefinitions().get(0);
      VectorizeConfig vectorizeConfig = null;
      if (vectorConfigColumn.vectorizeConfig() != null) {
        Map<String, String> authentication = vectorConfigColumn.vectorizeConfig().authentication();
        Map<String, Object> parameters = vectorConfigColumn.vectorizeConfig().parameters();
        vectorizeConfig =
            new VectorizeConfig(
                vectorConfigColumn.vectorizeConfig().provider(),
                vectorConfigColumn.vectorizeConfig().modelName(),
                authentication == null ? null : Map.copyOf(authentication),
                parameters == null ? null : Map.copyOf(parameters));
      }
      vectorSearchConfig =
          new CreateCollectionCommand.Options.VectorSearchConfig(
              vectorConfigColumn.vectorSize(),
              vectorConfigColumn.similarityFunction().name().toLowerCase(),
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
    final CollectionIdType idType = collectionSetting.idConfig().idType();
    CreateCollectionCommand.Options.IdConfig idConfig =
        (idType == null || idType == CollectionIdType.UNDEFINED)
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

  public CollectionIndexingConfig indexingConfig() {
    return indexingConfig;
  }

  // TODO: these helper functions break encapsulation for very little benefit
  public SimilarityFunction similarityFunction() {
    return vectorConfig().columnVectorDefinitions().get(0).similarityFunction();
  }

  public boolean isVectorEnabled() {
    return vectorConfig().vectorEnabled();
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
