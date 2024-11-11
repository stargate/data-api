package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.table.IndexDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.VectorIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorIndexDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.util.defaults.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiVectorIndex extends ApiSupportedIndex {

  public static final IndexFactoryFromIndexDesc<ApiVectorIndex, VectorIndexDefinitionDesc>
      FROM_DESC_FACTORY = new UserDescFactory();
  public static final IndexFactoryFromCql FROM_CQL_FACTORY = new CqlTypeFactory();

  public static final DefaultString SIMILARITY_FUNCTION_DEFAULT =
      Defaults.of(VectorIndexDescDefaults.DEFAULT_METRIC_NAME);

  public static final DefaultString SOURCE_MODEL_DEFAULT =
      Defaults.of(EmbeddingSourceModel.DEFAULT.apiName());

  private interface Options {
    // No default when we read this from the options map, we cannot guess what it is
    // The default above is for when we create a new index from user input
    StringProperty SIMILARITY_FUNCTION =
        Properties.ofRequired(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION);

    StringProperty SOURCE_MODEL =
        Properties.of(
            VectorConstants.CQLAnnIndex.SOURCE_MODEL, EmbeddingSourceModel.DEFAULT.apiName());
  }

  // because we actually use the similarityFunction when doing reads better to avoid checking the
  // options where it is
  // only a string. And for that, and the model, they have different names for different usages, so
  // to reduce
  // confusion we hold them here, and rely on the factory to sort it out.
  private final SimilarityFunction similarityFunction;
  private final EmbeddingSourceModel sourceModel;

  private ApiVectorIndex(
      CqlIdentifier indexName,
      CqlIdentifier targetColumn,
      Map<String, String> options,
      SimilarityFunction similarityFunction,
      EmbeddingSourceModel sourceModel) {
    super(ApiIndexType.VECTOR, indexName, targetColumn, options);

    this.similarityFunction = similarityFunction;
    this.sourceModel = sourceModel;
  }

  @Override
  public IndexDesc<VectorIndexDefinitionDesc> indexDesc() {

    var definitionOptions =
        new VectorIndexDefinitionDesc.VectorIndexDescOptions(
            similarityFunction.apiName(), sourceModel.apiName());

    var definition =
        new VectorIndexDefinitionDesc(cqlIdentifierToJsonKey(targetColumn), definitionOptions);

    return new IndexDesc<>() {
      @Override
      public String name() {
        return cqlIdentifierToJsonKey(indexName);
      }

      @Override
      public VectorIndexDefinitionDesc definition() {
        return definition;
      }
    };
  }

  public SimilarityFunction similarityFunction() {
    return similarityFunction;
  }

  /**
   * Factory to create a new {@link ApiVectorIndex} using {@link RegularIndexDefinitionDesc} from
   * the user request.
   */
  private static class UserDescFactory
      extends IndexFactoryFromIndexDesc<ApiVectorIndex, VectorIndexDefinitionDesc> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDescFactory.class);

    @Override
    public ApiVectorIndex create(
        TableSchemaObject tableSchemaObject,
        String indexName,
        VectorIndexDefinitionDesc indexDesc) {

      Objects.requireNonNull(tableSchemaObject, "tableSchemaObject must not be null");
      Objects.requireNonNull(indexDesc, "indexDesc must not be null");

      // for now we are relying on the validation of the request deserializer that these values are
      // specified userNameToIdentifier will throw an exception if the values are not specified
      var indexIdentifier = userNameToIdentifier(indexName, "indexName");
      var targetIdentifier = userNameToIdentifier(indexDesc.column(), "targetColumn");

      var apiColumnDef = checkIndexColumnExists(tableSchemaObject, targetIdentifier);

      // we could check if there is an existing index but that is a race condition, we will need to
      // catch it if it fails

      // Vector indexes can only be on vector columns
      if (apiColumnDef.type().typeName() != ApiTypeName.VECTOR) {
        throw SchemaException.Code.VECTOR_INDEX_NOT_SUPPORTED_BY_DATA_TYPE.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put(
                      "allColumns",
                      errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                  map.put("unsupportedColumns", errFmt(apiColumnDef));
                }));
      }

      Map<String, String> indexOptions = new HashMap<>();

      // Work out the source model, we need to use defaults and catch bad model names from the user
      // Use the default for the property, the only way we don't get a source mode is if the name
      // was unknown
      var userOrDefaultModelName =
          SOURCE_MODEL_DEFAULT.apply(
              indexDesc.options(), VectorIndexDefinitionDesc.VectorIndexDescOptions::sourceModel);
      var maybeSourceModel = EmbeddingSourceModel.fromApiName(userOrDefaultModelName);
      if (maybeSourceModel.isEmpty()) {
        throw SchemaException.Code.UNKNOWN_VECTOR_SOURCE_MODEL.get(
            Map.of(
                "knownSourceModels",
                EmbeddingSourceModel.getSupportedSourceModelNames(),
                "unknownSourceModel",
                userOrDefaultModelName));
      }
      var sourceModelToUse = maybeSourceModel.get();
      Options.SOURCE_MODEL.putOrDefault(indexOptions, sourceModelToUse.cqlName());

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "create() - options.sourceModel: {}, userOrDefaultModelName: {}, sourceModelToUse: {} ",
            (indexDesc.options() == null) ? "<options null>" : indexDesc.options().sourceModel(),
            userOrDefaultModelName,
            sourceModelToUse);
      }

      // Work out the similarity function, we need to use defaults and catch bad names from the
      // user,
      // and fall back to the source model if the user did not provide a function
      // Using the default value, get the similarity function from the options
      var userOrDefaultFunctionName =
          SIMILARITY_FUNCTION_DEFAULT.apply(
              indexDesc.options(), VectorIndexDefinitionDesc.VectorIndexDescOptions::metric);
      // get the enum, the only way this fails is if the name is unknown
      var userOrDefaultFunction = SimilarityFunction.fromApiName(userOrDefaultFunctionName);

      if (userOrDefaultFunction.isEmpty()) {
        throw SchemaException.Code.UNKNOWN_VECTOR_METRIC.get(
            Map.of(
                "knownMetrics",
                errFmtJoin(List.of(SimilarityFunction.values()), SimilarityFunction::apiName),
                "unknownMetric",
                userOrDefaultFunctionName));
      }

      // Now need to work out what function we use, the one the user provided or the one from the
      // source model.
      var functionToUse =
          SimilarityFunction.decideFromInputOrModel(
              SIMILARITY_FUNCTION_DEFAULT.isPresent(
                  indexDesc.options(), VectorIndexDefinitionDesc.VectorIndexDescOptions::metric),
              userOrDefaultFunction.orElse(null),
              maybeSourceModel.get());

      // Unlike the source model, the similarity function is required, and because we are building
      // the index we use the cqlIndexingFunction()
      Options.SIMILARITY_FUNCTION.putOrDefault(indexOptions, functionToUse.cqlIndexingFunction());

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "create() - options.metric: {}, userOrDefaultFunctionName: {}, functionToUse: {} ",
            (indexDesc.options() == null) ? "<options null>" : indexDesc.options().metric(),
            userOrDefaultFunctionName,
            functionToUse);
      }

      return new ApiVectorIndex(
          indexIdentifier, targetIdentifier, indexOptions, functionToUse, sourceModelToUse);
    }
  }

  /**
   * Factory to create a new {@link ApiVectorIndex} using the {@link IndexMetadata} from the driver.
   */
  private static class CqlTypeFactory extends IndexFactoryFromCql {

    @Override
    protected ApiIndexDef create(
        ApiColumnDef apiColumnDef, CQLSAIIndex.IndexTarget indexTarget, IndexMetadata indexMetadata)
        throws UnsupportedCqlIndexException {

      // this is a sanity check, the base will have worked this, but we should check it here
      var apiIndexType = ApiIndexType.fromCql(apiColumnDef, indexTarget, indexMetadata);
      if (apiIndexType != ApiIndexType.VECTOR) {
        throw new IllegalStateException(
            "ApiVectorIndex factory only supports %s indexes, apiIndexType: %s"
                .formatted(ApiIndexType.VECTOR, apiIndexType));
      }

      // also, we  must not have an index function
      if (indexTarget.indexFunction() != null) {
        throw new IllegalStateException(
            "ApiVectorIndex factory must not have index function, indexMetadata.name: "
                + indexMetadata.getName());
      }

      var sourceModel =
          EmbeddingSourceModel.fromCqlName(
                  Options.SOURCE_MODEL.getWithDefault(indexMetadata.getOptions()))
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Unknown source model index.name:%s, index.options%s"
                              .formatted(indexMetadata.getName(), indexMetadata.getOptions())));

      var similarityFunction =
          SimilarityFunction.fromCqlIndexingFunction(
                  Options.SIMILARITY_FUNCTION.getWithDefault(indexMetadata.getOptions()))
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Unknown similarity function index.name:%s, index.options%s"
                              .formatted(indexMetadata.getName(), indexMetadata.getOptions())));

      return new ApiVectorIndex(
          indexMetadata.getName(),
          indexTarget.targetColumn(),
          indexMetadata.getOptions(),
          similarityFunction,
          sourceModel);
    }
  }
}
