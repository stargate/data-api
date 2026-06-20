package io.stargate.sgv2.jsonapi.service.schema.collections;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.schema.*;
import io.stargate.sgv2.jsonapi.service.schema.CollectionSchemaVersion;
import io.stargate.sgv2.jsonapi.service.schema.SchemaHolder;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Refactored as separate class that represent a collection property.
 *
 * <p>TODO: there are a LOT of different ways this is constructed, need to refactor
 */
public final class CollectionSchemaObject extends TableBasedSchemaObject {

  private final IdConfig idConfig;
  private final VectorConfig vectorConfig;
  private final CollectionIndexingConfig indexingConfig;
  private final TableMetadata tableMetadata;
  private final SchemaHolder<CollectionLexicalDef> lexicalDef;
  private final SchemaHolder<CollectionRerankDef> rerankDef;

  public CollectionSchemaObject(
      Tenant tenant,
      TableMetadata tableMetadata,
      IdConfig idConfig,
      VectorConfig vectorConfig,
      CollectionIndexingConfig indexingConfig,
      SchemaHolder<CollectionLexicalDef> lexicalDef,
      SchemaHolder<CollectionRerankDef> rerankDef) {

    super(SchemaObjectType.COLLECTION, tenant, tableMetadata);

    this.idConfig = idConfig;
    this.vectorConfig = vectorConfig;
    this.indexingConfig = indexingConfig;
    this.tableMetadata = tableMetadata;
    this.lexicalDef = Objects.requireNonNull(lexicalDef);
    this.rerankDef = Objects.requireNonNull(rerankDef);
  }

  /**
   * we have a lot of old tests that created a collection without having table metadata. Use the
   * ctor with TableMetadata in prod code
   */
  @VisibleForTesting
  public CollectionSchemaObject(
      SchemaObjectIdentifier identifier,
      IdConfig idConfig,
      VectorConfig vectorConfig,
      CollectionIndexingConfig indexingConfig,
      SchemaHolder<CollectionLexicalDef> lexicalDef,
      SchemaHolder<CollectionRerankDef> rerankDef) {

    super(SchemaObjectType.COLLECTION, identifier);

    this.idConfig = idConfig;
    this.vectorConfig = vectorConfig;
    this.indexingConfig = indexingConfig;
    this.tableMetadata = null;
    this.lexicalDef = Objects.requireNonNull(lexicalDef);
    this.rerankDef = Objects.requireNonNull(rerankDef);
  }

  @Override
  public VectorConfig vectorConfig() {
    return vectorConfig;
  }

  @Override
  public IndexUsage newIndexUsage() {
    return newCollectionIndexUsage();
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder)
        .append("idConfig", idConfig)
        .append("vectorConfig", vectorConfig)
        .append("indexingConfig", indexingConfig)
        .append("lexicalDef", lexicalDef.runningValue())
        .append("rerankDef", rerankDef.runningValue());
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

  public static CollectionSchemaObject getCollectionSettings(
      RequestContext requestContext, TableMetadata table, ObjectMapper objectMapper) {

    // get vector column
    final Optional<ColumnMetadata> vectorColumn =
        table.getColumn(SuperShreddingMetadata.Names.QUERY_VECTOR_VALUE);
    boolean vectorEnabled = vectorColumn.isPresent();
    final String comment = (String) table.getOptions().get(CqlIdentifier.fromInternal("comment"));

    // if vector column exists
    if (vectorEnabled) {
      final int vectorSize = ((VectorType) vectorColumn.get().getType()).getDimensions();
      // get vector index
      IndexMetadata vectorIndex = null;
      Map<CqlIdentifier, IndexMetadata> indexMap = table.getIndexes();
      for (CqlIdentifier key : indexMap.keySet()) {
        if (key.asInternal().endsWith(SuperShreddingMetadata.Names.QUERY_VECTOR_VALUE)) {
          vectorIndex = indexMap.get(key);
          break;
        }
      }
      // default function and source model
      SimilarityFunction function = SimilarityFunction.COSINE;
      EmbeddingSourceModel sourceModel = EmbeddingSourceModel.OTHER;
      if (vectorIndex != null) {
        final String functionName =
            vectorIndex.getOptions().get(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION);
        final String sourceModelName =
            vectorIndex.getOptions().get(VectorConstants.CQLAnnIndex.SOURCE_MODEL);
        if (functionName != null) {
          function =
              SimilarityFunction.fromCqlIndexingFunction(functionName)
                  .orElseThrow(() -> SimilarityFunction.getUnknownFunctionException(functionName));
        }
        if (sourceModelName != null) {
          sourceModel =
              EmbeddingSourceModel.fromApiNameOrDefault(sourceModelName)
                  .orElseThrow(
                      () -> EmbeddingSourceModel.getUnknownSourceModelException(sourceModelName));
        }
      }

      return createCollectionSettings(
          requestContext, table, true, vectorSize, function, sourceModel, comment, objectMapper);
    } else { // if not vector collection
      return createCollectionSettings(
          requestContext,
          table,
          false,
          0,
          SimilarityFunction.DEFAULT,
          EmbeddingSourceModel.DEFAULT,
          comment,
          objectMapper);
    }
  }

  public static CollectionSchemaObject createCollectionSettings(
      RequestContext requestContext,
      TableMetadata tableMetadata,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction function,
      EmbeddingSourceModel sourceModel,
      String comment,
      ObjectMapper objectMapper) {

    var schemaHolder = readCollectionSchema(objectMapper, tableMetadata, comment);

    return switch (schemaHolder.version()) {
      case V_minus ->
          createCollectionSchemaVersionMinus(
              requestContext, tableMetadata, vectorEnabled, vectorSize, function, sourceModel);
      case V_0 ->
          new CollectionSettingsV0Reader()
              .readCollectionSettings(
                  requestContext,
                  schemaHolder.collectionNode(),
                  tableMetadata,
                  vectorEnabled,
                  vectorSize,
                  function,
                  sourceModel);
      case V_1 ->
          new CollectionSettingsV1Reader()
              .readCollectionSettings(
                  requestContext, schemaHolder.collectionNode(), tableMetadata, objectMapper);
      case V_2 ->
          new CollectionSettingsV2Reader()
              .readCollectionSettings(
                  requestContext, schemaHolder.collectionNode(), tableMetadata, objectMapper);
    };
  }

  private static CollectionSchemaHolder readCollectionSchema(
      ObjectMapper objectMapper, TableMetadata tableMetadata, String tableComment) {

    // ## VERSION MINUS - No schema at all
    if (tableComment == null || tableComment.isBlank()) {
      // No table comment at all, nothing in the comment for the table.
      // no schema tracking at all
      return new CollectionSchemaHolder(CollectionSchemaVersion.V_minus, null);
    }

    JsonNode commentConfigNode;
    try {
      commentConfigNode = objectMapper.readTree(tableComment);
    } catch (JacksonException e) {
      // This should never happen, already check if vectorize is a valid JSON
      throw ServerException.internalServerError(
          "Invalid JSON in Table comment for Collection, problem: " + e.getMessage());
    }

    // new table comment design from schema_version v1, with collection as top-level key
    var collectionNode = commentConfigNode.get(TableCommentConstants.TOP_LEVEL_KEY);

    // ## VERSION ZERO - we have a table comment that is json, but does not have
    // 'collection' as top key
    // backward compatibility for old indexing table comment
    // sample comment : {"indexing":{"deny":["address"]}}}
    if (collectionNode == null) {
      return new CollectionSchemaHolder(CollectionSchemaVersion.V_0, commentConfigNode);
    }

    // ## VERSION 1 AND ABOVE
    // we have a "collection" top level key, so we should have a "schema_version" under that we can
    // read !
    var schemaVersionNode = collectionNode.get(TableCommentConstants.SCHEMA_VERSION_KEY);
    if (schemaVersionNode == null) {
      throw DatabaseException.Code.COLLECTION_SCHEMA_VERSION_INVALID.get(
          Map.of(
              "collectionName", tableMetadata.getName().asInternal(), "schemaVersion", "<null>"));
    }

    int schemaVersion = schemaVersionNode.asInt();
    return switch (schemaVersion) {
      case 1 -> new CollectionSchemaHolder(CollectionSchemaVersion.V_1, collectionNode);
      case 2 -> new CollectionSchemaHolder(CollectionSchemaVersion.V_2, collectionNode);
      default ->
          throw DatabaseException.Code.COLLECTION_SCHEMA_VERSION_INVALID.get(
              Map.of(
                  "collectionName",
                  tableMetadata.getName().asInternal(),
                  "schemaVersion",
                  String.valueOf(schemaVersion)));
    };
  }

  private record CollectionSchemaHolder(CollectionSchemaVersion version, JsonNode collectionNode) {}

  /**
   * how we make the CollectionSchemaObject when there was no table comment, this is version minus
   */
  private static CollectionSchemaObject createCollectionSchemaVersionMinus(
      RequestContext requestContext,
      TableMetadata tableMetadata,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction function,
      EmbeddingSourceModel sourceModel) {

    var lexicalConfig =
        requestContext
            .schemaRegistry()
            .lexicalDef()
            .namedVersion(CollectionSchemaVersion.V_minus, null);

    var rerankingConfig =
        requestContext
            .schemaRegistry()
            .rerankDef()
            .namedVersion(CollectionSchemaVersion.V_minus, null);

    VectorConfig vectorConfig =
        vectorEnabled
            ? VectorConfig.fromColumnDefinitions(
                List.of(
                    new VectorColumnDefinition(
                        VECTOR_EMBEDDING_TEXT_FIELD, vectorSize, function, sourceModel, null)))
            : VectorConfig.NOT_ENABLED_CONFIG;

    return new CollectionSchemaObject(
        requestContext.tenant(),
        tableMetadata,
        IdConfig.defaultIdConfig(),
        vectorConfig,
        null,
        lexicalConfig,
        rerankingConfig);
  }

  public static CreateCollectionCommand collectionSettingToCreateCollectionCommand(
      CollectionSchemaObject collectionSetting) {

    // TODO: move the vector and vectorize parts to be methods on those schema objects
    CreateCollectionCommand.Options options;
    CreateCollectionCommand.Options.VectorSearchDesc vectorSearchDesc = null;
    CreateCollectionCommand.Options.IndexingDesc indexingDesc = null;

    // populate the vectorSearchConfig, Default will be the index 0 since there is only one vector
    // column supported for collection
    final VectorConfig vectorConfig = collectionSetting.vectorConfig();
    if (vectorConfig.vectorEnabled()) {

      // checked above that vector is enabled
      var vectorColumnDefinition =
          vectorConfig.getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD).orElseThrow();
      VectorizeConfig vectorizeConfig = null;

      if (vectorColumnDefinition.vectorizeDefinition() != null) {
        Map<String, String> authentication =
            vectorColumnDefinition.vectorizeDefinition().authentication();
        Map<String, Object> parameters = vectorColumnDefinition.vectorizeDefinition().parameters();
        vectorizeConfig =
            new VectorizeConfig(
                vectorColumnDefinition.vectorizeDefinition().provider(),
                vectorColumnDefinition.vectorizeDefinition().modelName(),
                authentication == null ? null : Map.copyOf(authentication),
                parameters == null ? null : Map.copyOf(parameters));
      }

      vectorSearchDesc =
          new CreateCollectionCommand.Options.VectorSearchDesc(
              vectorColumnDefinition.vectorSize(),
              vectorColumnDefinition.similarityFunction().name().toLowerCase(),
              vectorColumnDefinition.sourceModel().apiName(),
              vectorizeConfig);
    }

    // populate the indexingConfig
    if (collectionSetting.indexingConfig() != null) {
      indexingDesc =
          new CreateCollectionCommand.Options.IndexingDesc(
              Lists.newArrayList(collectionSetting.indexingConfig().allowed()),
              Lists.newArrayList(collectionSetting.indexingConfig().denied()));
    }

    // construct the CreateCollectionCommand.options.idConfig -- but only if non-default IdType
    final CollectionIdType idType = collectionSetting.idConfig().idType();
    CreateCollectionCommand.Options.DocIdDesc idConfig =
        (idType == null || idType == CollectionIdType.UNDEFINED)
            ? null
            : new CreateCollectionCommand.Options.DocIdDesc(idType.toString());

    // construct the CreateCollectionCommand.options.lexicalConfig
    // using the runningValue because this is what is used for DML ops
    var lexicalDesc = collectionSetting.lexicalDef().toApiDesc();

    // construct the CreateCollectionCommand.options.rerankDef
    var rerankDesc = collectionSetting.rerankDef().toApiDesc();

    options =
        new CreateCollectionCommand.Options(
            idConfig, vectorSearchDesc, indexingDesc, lexicalDesc, rerankDesc);

    // CreateCollectionCommand object is created for convenience to generate json
    // response. The code is not creating a collection here.
    return new CreateCollectionCommand(
        collectionSetting.identifier().table().asInternal(), options);
  }

  public IdConfig idConfig() {
    return idConfig;
  }

  public CollectionIndexingConfig indexingConfig() {
    return indexingConfig;
  }

  public CollectionLexicalDef lexicalDef() {
    return lexicalDef.runningValue();
  }

  public SchemaHolder<CollectionLexicalDef> lexicalDefSchemaValue() {
    return lexicalDef;
  }

  public CollectionRerankDef rerankDef() {
    return rerankDef.runningValue();
  }

  public SchemaHolder<CollectionRerankDef> rerankDefSchemaValue() {
    return rerankDef;
  }

  // TODO: these helper functions break encapsulation for very little benefit
  public SimilarityFunction similarityFunction() {
    // TODO: THERE WAS NO CHECK HERE IF VECTORING WAS ENABLED
    return vectorConfig()
        .getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD)
        .get()
        .similarityFunction();
  }

  // TODO: the overrides below were auto added when migrating from a record to a class, not sure
  // they are needed or wanted
  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (CollectionSchemaObject) obj;

    // using explicit calls to runningValue() for the SchemaHolder to make it clear
    // we use the value of the schema object. Even though the SchemaHolder equals
    // also uses the runningValue()
    return Objects.equals(this.identifier(), that.identifier())
        && Objects.equals(this.idConfig, that.idConfig)
        && Objects.equals(this.vectorConfig, that.vectorConfig)
        && Objects.equals(this.indexingConfig, that.indexingConfig)
        && Objects.equals(this.lexicalDef.runningValue(), that.lexicalDef.runningValue())
        && Objects.equals(this.rerankDef.runningValue(), that.rerankDef.runningValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        identifier(),
        idConfig,
        vectorConfig,
        indexingConfig,
        lexicalDef.runningValue(),
        rerankDef.runningValue());
  }

  @Override
  public String toString() {
    return "CollectionSchemaObject["
        + "identifier="
        + identifier()
        + ", "
        + "idConfig="
        + idConfig
        + ", "
        + "vectorConfig="
        + vectorConfig
        + ", "
        + "indexingConfig="
        + indexingConfig
        + ", "
        + "lexicalDef="
        + lexicalDef.runningValue()
        + ", "
        + "rerankDef="
        + rerankDef.runningValue()
        + ']';
  }
}
