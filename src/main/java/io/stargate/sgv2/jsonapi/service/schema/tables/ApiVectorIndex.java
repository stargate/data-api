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
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiVectorIndex extends ApiSupportedIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiVectorIndex.class);

  public static final IndexFactoryFromIndexDesc<ApiVectorIndex, VectorIndexDefinitionDesc>
      FROM_DESC_FACTORY = new UserDescFactory();
  public static final IndexFactoryFromCql FROM_CQL_FACTORY = new CqlTypeFactory();

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
   * Logic to map from the name of the similarity function, from either the user or the CQL index,
   * to a {@link SimilarityFunction} enum value.
   *
   * @param functionName The raw name from the user input or CQL index, can be null or empty
   * @param indexMetadata if the functionname came from driver {@link IndexMetadata} provide this,
   *     otherwise null
   * @return Optional of the similarity function, empty if the user did not provide a name
   */
  private static Optional<SimilarityFunction> similarityFunctionFromName(
      String functionName, IndexMetadata indexMetadata) {

    if (functionName == null || functionName.isBlank()) {
      // nothing provided, so we return empty. There is no default function
      return Optional.empty();
    }

    // calling from without the default means we will get an empty optional if the name is unknown
    // we already checked above to for null, which would also result in an empty.
    // so any empty means the name is provied and unknown
    var userMetric =
        (indexMetadata == null)
            ? SimilarityFunction.fromApiName(functionName)
            : SimilarityFunction.fromCqlIndexingFunction(functionName);

    if (userMetric.isPresent()) {
      return userMetric;
    }

    if (indexMetadata == null) {
      // this request came from the user, so use user errors
      throw SchemaException.Code.UNKNOWN_VECTOR_METRIC.get(
          Map.of(
              "knownMetrics",
              errFmtJoin(List.of(SimilarityFunction.values()), SimilarityFunction::apiName),
              "unknownMetric",
              functionName));
    }

    // we have index metadata, it came from the driver, so an illegal state not a user error
    throw new IllegalStateException(
        "Unknown similarity function name: %s, index.name:%s, index.options%s"
            .formatted(functionName, indexMetadata.getName(), indexMetadata.getOptions()));
  }

  private static EmbeddingSourceModel sourceModelFromName(
      String modelName, IndexMetadata indexMetadata) {

    LOGGER.warn(
        "sourceModelFromName() - modelName: {}, indexMetadata: {}", modelName, indexMetadata);
    // if the provided name is null or blank we will get the default
    var maybeSourceModel =
        (indexMetadata == null)
            ? EmbeddingSourceModel.fromApiNameOrDefault(modelName)
            : EmbeddingSourceModel.fromCqlNameOrDefault(modelName);

    if (maybeSourceModel.isPresent()) {
      return maybeSourceModel.get();
    }

    // the only way to not have a source model is a name was provided that is not known
    if (indexMetadata == null) {
      // request came from users, so use user errors
      throw SchemaException.Code.UNKNOWN_VECTOR_SOURCE_MODEL.get(
          Map.of(
              "knownSourceModels",
              errFmtJoin(EmbeddingSourceModel.allApiNames()),
              "unknownSourceModel",
              modelName));
    }

    throw new IllegalStateException(
        "Unknown source model name: %s, index.name:%s, index.options%s"
            .formatted(modelName, indexMetadata.getName(), indexMetadata.getOptions()));
  }

  /**
   * The metric we will use will be the one from the user, or the one from the model if user did not
   * specify metric we always have a model.
   *
   * @param sourceModel The source model we are using.
   * @param userMetric Optional metric from the user.
   * @return The metric to use.
   */
  private static SimilarityFunction decideSimilarityFunction(
      EmbeddingSourceModel sourceModel, Optional<SimilarityFunction> userMetric) {
    Objects.requireNonNull(sourceModel, "sourceModel must not be null");
    return userMetric.orElse(sourceModel.similarityFunction());
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
        throw SchemaException.Code.UNSUPPORTED_VECTOR_INDEX_FOR_DATA_TYPES.get(
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

      // Work out the source model, we always have one even if the user did not provide one
      var userModelName = (indexDesc.options() == null) ? null : indexDesc.options().sourceModel();
      var sourceModelToUse = sourceModelFromName(userModelName, null);
      // we always write the source model to the options
      indexOptions.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, sourceModelToUse.cqlName());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "create() - userModelName: {}, sourceModelToUse: {} ", userModelName, sourceModelToUse);
      }

      // The user can provide a similarity function, if they do not we use the one for the model.
      var userMetricName = (indexDesc.options() == null) ? null : indexDesc.options().metric();
      var maybeUserMetric = similarityFunctionFromName(userMetricName, null);
      // we only have one if the user specified one and it was valid, store in the options if this
      // is the case
      maybeUserMetric.ifPresent(
          metric ->
              indexOptions.put(
                  VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION, metric.cqlIndexingFunction()));
      var metricToUse = decideSimilarityFunction(sourceModelToUse, maybeUserMetric);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "create() - userMetricName: {}, maybeUserMetric: {}, metricToUse: {}",
            userMetricName,
            maybeUserMetric,
            metricToUse);
      }

      return new ApiVectorIndex(
          indexIdentifier, targetIdentifier, indexOptions, metricToUse, sourceModelToUse);
    }
  }

  /**
   * Factory to create a new {@link ApiVectorIndex} using the {@link IndexMetadata} from the driver.
   */
  private static class CqlTypeFactory extends IndexFactoryFromCql {
    private static final Logger LOGGER = LoggerFactory.getLogger(CqlTypeFactory.class);

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

      var indexModelName = indexMetadata.getOptions().get(VectorConstants.CQLAnnIndex.SOURCE_MODEL);
      var indexModelToUse = sourceModelFromName(indexModelName, indexMetadata);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "create() - indexModelName: {}, indexModelToUse: {} ", indexModelName, indexModelToUse);
      }

      var indexMetricName =
          indexMetadata.getOptions().get(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION);
      var maybeIndexMetric = similarityFunctionFromName(indexMetricName, indexMetadata);
      var indexMetricToUse = decideSimilarityFunction(indexModelToUse, maybeIndexMetric);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "create() - indexMetricName: {}, maybeIndexMetric: {}, indexMetricToUse: {}",
            indexMetricName,
            maybeIndexMetric,
            indexMetricToUse);
      }

      return new ApiVectorIndex(
          indexMetadata.getName(),
          indexTarget.targetColumn(),
          indexMetadata.getOptions(),
          indexMetricToUse,
          indexModelToUse);
    }
  }
}