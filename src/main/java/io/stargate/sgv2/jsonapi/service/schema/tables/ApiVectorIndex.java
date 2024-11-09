package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.Map;
import java.util.Objects;

public class ApiVectorIndex extends ApiRegularIndex {

  public static final IndexFactoryFromCql FROM_CQL_FACTORY = new CqlTypeFactory();

  private final SimilarityFunction similarityFunction;

  ApiVectorIndex(
      CqlIdentifier indexName,
      CqlIdentifier targetColumn,
      Map<String, String> options,
      SimilarityFunction similarityFunction) {
    super(indexName, targetColumn, options);
    this.similarityFunction =
        Objects.requireNonNull(similarityFunction, "similarityFunction must not be null");
  }

  @Override
  public ApiIndexType indexType() {
    return ApiIndexType.VECTOR;
  }

  public SimilarityFunction similarityFunction() {
    return similarityFunction;
  }

  private static class CqlTypeFactory extends IndexFactoryFromCql {

    @Override
    protected ApiIndexDef create(
        ApiColumnDef apiColumnDef, IndexTarget indexTarget, IndexMetadata indexMetadata)
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
          SimilarityFunction.fromCqlName(
              CqlIndexOptions.SIMILARITY_FUNCTION.readFrom(indexMetadata.getOptions()));
      return new ApiVectorIndex(
          indexMetadata.getName(),
          indexTarget.targetColumn(),
          indexMetadata.getOptions(),
          similarityFunction);
    }
  }
}
