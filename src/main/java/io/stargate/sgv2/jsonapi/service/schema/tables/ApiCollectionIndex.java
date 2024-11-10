package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.IndexDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import java.util.Map;
import java.util.Objects;

public class ApiCollectionIndex extends ApiSupportedIndex {

  public static final IndexFactoryFromCql FROM_CQL_FACTORY = new CqlTypeFactory();

  // indexes on list and sets always use the {@link ApiIndexFunction#VALUES}
  private ApiIndexFunction indexFunction;

  ApiCollectionIndex(
      CqlIdentifier indexName,
      CqlIdentifier targetColumn,
      Map<String, String> options,
      ApiIndexFunction indexFunction) {
    super(ApiIndexType.COLLECTION, indexName, targetColumn, options);
    this.indexFunction = Objects.requireNonNull(indexFunction, "indexFunction must not be null");
  }

  public ApiIndexFunction indexFunction() {
    return indexFunction;
  }

  @Override
  public IndexDesc indexDesc() {
    // TODO: implement this
    return null;
  }

  private static class CqlTypeFactory extends IndexFactoryFromCql {

    @Override
    protected ApiIndexDef create(
        ApiColumnDef apiColumnDef, CQLSAIIndex.IndexTarget indexTarget, IndexMetadata indexMetadata)
        throws UnsupportedCqlIndexException {

      // this is a sanity check, the base will have worked this, but we should check it here
      var apiIndexType = ApiIndexType.fromCql(apiColumnDef, indexTarget, indexMetadata);
      if (apiIndexType != ApiIndexType.COLLECTION) {
        throw new IllegalStateException(
            "ApiCollectionIndex factory only supports %s indexes, apiIndexType: %s"
                .formatted(ApiIndexType.COLLECTION, apiIndexType));
      }

      // also, we  must have an index function
      if (indexTarget.indexFunction() == null) {
        throw new IllegalStateException(
            "ApiCollectionIndex factory must have index function, indexMetadata.name: "
                + indexMetadata.getName());
      }

      return new ApiCollectionIndex(
          indexMetadata.getName(),
          indexTarget.targetColumn(),
          indexMetadata.getOptions(),
          indexTarget.indexFunction());
    }
  }
}
