package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownCqlIndexFunctionException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for creating an {@link ApiIndexDef} from a {@link IndexMetadata} from the driver.
 *
 * <p>Call the {@link #create(ApiColumnDefContainer, IndexMetadata)} to get an index.
 *
 * <p>Factories for specific index types should implement {@link #create(ApiColumnDef,
 * CQLSAIIndex.IndexTarget, IndexMetadata)}
 */
public abstract class IndexFactoryFromCql extends FactoryFromCql {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexFactoryFromCql.class);

  /**
   * Analyses the IndexMetadat to call the correct factory to create an {@link ApiIndexDef}
   *
   * @param allColumns Container of all columns on the table the index is from.
   * @param indexMetadata The index metadata from the driver
   * @return An {@link ApiIndexDef} that represents the index, never null. If the index is not
   *     supported then an {@link UnsupportedIndex} will be returned.
   */
  public static ApiIndexDef create(ApiColumnDefContainer allColumns, IndexMetadata indexMetadata) {

    // This first check is to see if there is anyway we can support this index, because we are only
    // supporting SAI indexes, later on we may find something that we could support but don't
    if (!isSupported(indexMetadata)) {
      return createUnsupported(indexMetadata);
    }

    // if we get this far, there is a chance we can support it
    try {
      // will throw if we could not work out the target
      var indexTarget = CQLSAIIndex.indexTarget(indexMetadata);

      var apiColumnDef = allColumns.get(indexTarget.targetColumn());
      if (apiColumnDef == null) {
        // Cassandra should not let there be an index on a column that is not on the table
        throw new IllegalStateException(
            "Could not find target column index.name:%s target: %s"
                .formatted(indexMetadata.getName(), indexTarget.targetColumn()));
      }
      // will throw if we cannot work it out
      var apiIndexType = ApiIndexType.fromCql(apiColumnDef, indexTarget, indexMetadata);

      return switch (apiIndexType) {
        case REGULAR ->
            ApiRegularIndex.FROM_CQL_FACTORY.create(apiColumnDef, indexTarget, indexMetadata);
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

  /**
   * Specific index factories should implement this to create the index.
   *
   * @param apiColumnDef The column the index is on.
   * @param indexTarget The parsed information about the target and function the index uses.
   * @param indexMetadata The index metadata from the driver.
   * @return The index, never null.
   * @throws UnsupportedCqlIndexException Throw is the index cannot be supported, the caller will
   *     create a {@link UnsupportedIndex}
   */
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
  public static boolean isSupported(IndexMetadata indexMetadata) {
    return CQLSAIIndex.isSAIIndex(indexMetadata);
  }

  public static UnsupportedIndex createUnsupported(IndexMetadata indexMetadata) {
    return new UnsupportedIndex(indexMetadata);
  }
}
