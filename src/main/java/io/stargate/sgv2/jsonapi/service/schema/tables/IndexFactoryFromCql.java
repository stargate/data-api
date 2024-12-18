package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownCqlIndexFunctionException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public abstract class IndexFactoryFromCql extends FactoryFromCql {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexFactoryFromCql.class);

  public static final IndexFactoryFromCql DEFAULT = new DefaultFactory();

  public ApiIndexDef create(ApiColumnDefContainer allColumns, IndexMetadata indexMetadata)
      throws UnsupportedCqlIndexException {

    // This first check is to see if there is anyway we can support this index, becase we are only
    // supporting
    // SAI indexes, later on we may find something that we could support but dont like a new type of
    // sai
    if (!isSupported(indexMetadata)) {
      return createUnsupported(indexMetadata);
    }

    // if we get this var, there is a chance we can support it
    try {
      // will throw if we could not work out the target
      var indexTarget = CQLSAIIndex.indexTarget(indexMetadata);

      var apiColumnDef = allColumns.get(indexTarget.targetColumn());
      if (apiColumnDef == null) {
        // Cassandra should not let there be an index on a column that is not on the table
        throw new IllegalStateException(
            "Could not find target column index.name:%s target: "
                .formatted(indexMetadata.getName(), indexTarget.targetColumn()));
      }
      // will throw if we cannot work it out
      var apiIndexType = ApiIndexType.fromCql(apiColumnDef, indexTarget, indexMetadata);

      return switch (apiIndexType) {
        case REGULAR ->
            ApiRegularIndex.FROM_CQL_FACTORY.create(apiColumnDef, indexTarget, indexMetadata);
          // for now we do not support collection indexes, will do for GA - aaron nov 11
        case COLLECTION -> createUnsupported(indexMetadata);
        case VECTOR ->
            ApiVectorIndex.FROM_CQL_FACTORY.create(apiColumnDef, indexTarget, indexMetadata);
        default ->
            throw new UnsupportedCqlIndexException(
                "No index factory for type: " + apiIndexType, indexMetadata);
      };
    } catch (UnknownCqlIndexFunctionException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "create() - Could not work out the index function, creating UnsupportedCqlIndex", e);
      }
      return createUnsupported(indexMetadata);
    } catch (UnsupportedCqlIndexException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("create() - Could not create index, creating UnsupportedCqlIndex", e);
      }
      return createUnsupported(indexMetadata);
    }
  }

  protected abstract ApiIndexDef create(
      ApiColumnDef apiColumnDef, CQLSAIIndex.IndexTarget indexTarget, IndexMetadata indexMetadata)
      throws UnsupportedCqlIndexException;

  /**
   * We only support {@link IndexKind#CUSTOM} , these are from using CQL <code>CREATE CUSTOM INDEX
   * </code> and we only support SAI indexes, see example of the options in this class
   *
   * @param indexMetadata
   * @return
   */
  public boolean isSupported(IndexMetadata indexMetadata) {
    return CQLSAIIndex.isSAIIndex(indexMetadata);
  }

  public UnsupportedIndex createUnsupported(IndexMetadata indexMetadata) {
    return new UnsupportedIndex(indexMetadata);
  }

  private static class DefaultFactory extends IndexFactoryFromCql {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFactory.class);

    @Override
    protected ApiIndexDef create(
        ApiColumnDef apiColumnDef, CQLSAIIndex.IndexTarget indexTarget, IndexMetadata indexMetadata)
        throws UnsupportedCqlIndexException {
      throw new UnsupportedOperationException("create() - Not implemented");
    }
  }
}
