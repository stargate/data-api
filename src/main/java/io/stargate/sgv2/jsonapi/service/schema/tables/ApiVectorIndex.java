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
import java.util.Map;
import java.util.Objects;

public class ApiVectorIndex extends ApiSupportedIndex {

  public static final IndexFactoryFromIndexDesc<ApiVectorIndex, VectorIndexDefinitionDesc>
      FROM_DESC_FACTORY = new UserDescFactory();
  public static final IndexFactoryFromCql FROM_CQL_FACTORY = new CqlTypeFactory();

  public static final DefaultString SIMILARITY_FUNCTION_DEFAULT =
      Defaults.of(VectorIndexDescDefaults.DEFAULT_METRIC_NAME);

  // There is no de
  public static final DefaultString SOURCE_MODEL_DEFAULT = Defaults.of("");

  private interface Options {
    // No default when we read this from the options map, we cannot guess what it is
    // The default above is for when we create a new index from user input
    StringProperty SIMILARITY_FUNCTION =
        Properties.ofRequired(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION);

    // The default is a null string
    StringProperty SOURCE_MODEL =
        Properties.of(VectorConstants.CQLAnnIndex.SOURCE_MODEL, (String) null);
  }

  private final SimilarityFunction similarityFunction;

  private ApiVectorIndex(
      CqlIdentifier indexName,
      CqlIdentifier targetColumn,
      Map<String, String> options,
      SimilarityFunction similarityFunction) {
    super(ApiIndexType.VECTOR, indexName, targetColumn, options);

    // because we actually use the similarityFunction when doing reads, and it needs to be mapped to
    // an enum,
    // forcing the factories to do the work and pass it
    this.similarityFunction = similarityFunction;
  }

  @Override
  public IndexDesc<VectorIndexDefinitionDesc> indexDesc() {

    var definitionOptions =
        new VectorIndexDefinitionDesc.VectorIndexDescOptions(
            Options.SIMILARITY_FUNCTION.getWithDefault(indexOptions),
            Options.SOURCE_MODEL.getWithDefault(indexOptions));

    var definition =
        new VectorIndexDefinitionDesc(cqlIdentifierToJsonKey(targetColumn), definitionOptions);
    return new IndexDesc<VectorIndexDefinitionDesc>() {
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

    @Override
    public ApiVectorIndex create(
        TableSchemaObject tableSchemaObject,
        String indexName,
        VectorIndexDefinitionDesc indexDesc) {

      Objects.requireNonNull(tableSchemaObject, "tableSchemaObject must not be null");
      Objects.requireNonNull(indexDesc, "indexDesc must not be null");

      // for now we are relying on the validation of the request deserializer that these values are
      // specified
      // userNameToIdentifier will throw an exception if the values are not specified
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

      // TODO: throw if the model is not found
      // TODO: WHAT TO DO WITH UNDEFINED OR OTHER MODEL NAMES ???
      var maybeSourceModel =
          EmbeddingSourceModel.fromName(
              SOURCE_MODEL_DEFAULT.apply(
                  indexDesc.options(),
                  VectorIndexDefinitionDesc.VectorIndexDescOptions::sourceModel));

      // TODO: check the undefined model name

      // If we have a source model then set it in the options, otherwise leave it out
      maybeSourceModel.ifPresent(
          model -> Options.SOURCE_MODEL.putOrDefault(indexOptions, model.name()));

      var similarityFunctionName =
          SIMILARITY_FUNCTION_DEFAULT.apply(
              indexDesc.options(), VectorIndexDefinitionDesc.VectorIndexDescOptions::metric);
      var similarityFunction =
          SimilarityFunction.fromApiName(similarityFunctionName)
              .orElseThrow(() -> new IllegalStateException("TODO"));
      // TOOD: WHAT ABOUT the UNDEFINED similarity function ?

      // Unlike the source model, the similarity function is required, and because we are building
      // the index
      // we use the cqlIndexingFunction()
      Options.SIMILARITY_FUNCTION.putOrDefault(
          indexOptions, similarityFunction.cqlIndexingFunction());

      // TODO: aaron rationalise the source model  and similarity function

      return new ApiVectorIndex(
          indexIdentifier, targetIdentifier, indexOptions, similarityFunction);
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

      var similarityFunction =
          SimilarityFunction.fromCqlIndexingFunction(
                  Options.SIMILARITY_FUNCTION.getWithDefault(indexMetadata.getOptions()))
              .orElseThrow(() -> new IllegalStateException("TODO"));

      return new ApiVectorIndex(
          indexMetadata.getName(),
          indexTarget.targetColumn(),
          indexMetadata.getOptions(),
          similarityFunction);
    }
  }
}
