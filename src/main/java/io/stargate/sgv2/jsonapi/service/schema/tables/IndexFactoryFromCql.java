package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownCqlIndexFunctionException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public abstract class IndexFactoryFromCql extends FactoryFromCql {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexFactoryFromCql.class);

  /**
   * The target of the index is stored as a string in the indexing options in Cassandra 3.0+ See
   * {@link
   * com.datastax.oss.driver.internal.core.metadata.schema.parsing.TableParser#buildModernIndex(CqlIdentifier,
   * CqlIdentifier, AdminRow)} In the <code>system_schema.indexes</code> table the options can look
   * like:
   *
   * <pre>
   * options
   * ------------------------------------------------
   * {'class_name': 'StorageAttachedIndex', 'target': 'age'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'country'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(array_contains)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(array_size)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(exist_keys)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_bool_values)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_dbl_values)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(query_null_values)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_text_values)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_timestamp_values)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'comment_vector'}
   * {'class_name': 'StorageAttachedIndex', 'similarity_function': 'cosine', 'target': 'my_vector'}
   * </pre>
   *
   * The target can just be the name of the column, or the name of the column in parentheses
   * prefixed by the index type for a map type: values(column), keys(column), and entries(column)
   * see https://docs.datastax.com/en/cql/hcd-1.0/develop/indexing/sai/collections.html
   *
   * <p>The Reg Exp below will match:
   *
   * <ul>
   *   <li>"monkeys": group 1 - "monkeys"
   *   <li>"(monkeys)": group 1 - "monkeys" (without the parentheses) https://regex101.com/ called
   *       it group 2 but there was only 1
   *   <li>"values(monkeys)": group 1 - "values" group 2 - "monkeys"
   * </ul>
   */
  private static Pattern INDEX_TARGET_PATTERN = Pattern.compile("^(\\w+)?(?:\\((\\w+)\\))?$");

  // The name SAI uses for the {@link CqlIndexOptions#CLASS} option
  private static final String SAI_INDEX_CLASS = "StorageAttachedIndex";

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
      var indexTarget = IndexTarget.fromCql(indexMetadata);

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
        case COLLECTION ->
            ApiCollectionIndex.FROM_CQL_FACTORY.create(apiColumnDef, indexTarget, indexMetadata);
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
      ApiColumnDef apiColumnDef, IndexTarget indexTarget, IndexMetadata indexMetadata)
      throws UnsupportedCqlIndexException;

  /**
   * We only support {@link IndexKind#CUSTOM} , these are from using CQL <code>CREATE CUSTOM INDEX
   * </code> and we only support SAI indexes, see example of the options in this class
   *
   * @param indexMetadata
   * @return
   */
  public boolean isSupported(IndexMetadata indexMetadata) {
    return indexMetadata.getKind() == IndexKind.CUSTOM
        && SAI_INDEX_CLASS.equals(CqlIndexOptions.CLASS.readFrom(indexMetadata.getOptions()));
  }

  public UnsupportedCqlIndex createUnsupported(IndexMetadata indexMetadata) {
    return new UnsupportedCqlIndex(indexMetadata.getName(), indexMetadata.getOptions());
  }

  private static class DefaultFactory extends IndexFactoryFromCql {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFactory.class);

    @Override
    protected ApiIndexDef create(
        ApiColumnDef apiColumnDef, IndexTarget indexTarget, IndexMetadata indexMetadata)
        throws UnsupportedCqlIndexException {
      throw new UnsupportedOperationException("create() - Not implemented");
    }
  }

  /**
   * For internal to this package use only, for parsing the index target see docs for {@link
   * #INDEX_TARGET_PATTERN}
   *
   * @param targetColumn
   * @param indexFunction Null when there is no function
   */
  protected record IndexTarget(CqlIdentifier targetColumn, ApiIndexFunction indexFunction) {

    public static IndexTarget fromCql(IndexMetadata indexMetadata)
        throws UnknownCqlIndexFunctionException, UnsupportedCqlIndexException {

      var target = CqlIndexOptions.TARGET.readFrom(indexMetadata.getOptions());
      Matcher matcher = INDEX_TARGET_PATTERN.matcher(target);
      if (!matcher.matches()) {
        throw new UnsupportedCqlIndexException(
            "Could not parse index target: '" + target + "'", indexMetadata);
      }

      return switch (matcher.groupCount()) {
        case 1 -> new IndexTarget(CqlIdentifier.fromInternal(matcher.group(1)), null);
        case 2 ->
            new IndexTarget(
                CqlIdentifier.fromInternal(matcher.group(2)),
                ApiIndexFunction.fromCql(matcher.group(1)));
        default ->
            throw new IllegalArgumentException("Could not parse index target: '" + target + "'");
      };
    }
  }
}
