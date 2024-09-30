package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.IndexUsage;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.List;
import java.util.Map;

/**
 * A {@link DBFilterBase} that is applied to a table (i.e. not a Collection) to filter the rows to
 * read.
 */
public abstract class TableFilter extends DBFilterBase {

  // TIDY - the path is the column name here, maybe rename ?
  protected TableFilter(String path) {
    super(path, IndexUsage.NO_OP);
  }

  /**
   * Call to have the filter applied to the select statement using the {@link
   * com.datastax.oss.driver.api.querybuilder.QueryBuilder} from the Java driver.
   *
   * <p>NOTE: use this method rather than {@link DBFilterBase#get()} which is build to work with the
   * old gRPC bridge query builder.
   *
   * <p>TIDY: Refactor DBFilterBase to use this method when we move collection filters to use the
   * java driver.
   *
   * @param tableSchemaObject The table the filter is being applied to.
   * @param ongoingWhereClause The class from the Java Driver that implements the {@link
   *     OngoingWhereClause} that is used to build the WHERE in a CQL clause. This is the type of
   *     the statement the where is being added to such {@link Select} or {@link
   *     com.datastax.oss.driver.api.querybuilder.update.Update}
   * @param positionalValues Mutable array of values that are used when the {@link
   *     com.datastax.oss.driver.api.querybuilder.QueryBuilder#bindMarker()} method is used, the
   *     values are added to the select statement using {@link Select#build(Object...)}
   * @return The {@link Select} to use to continue building the query. NOTE: the query builder is a
   *     fluent builder that returns immutable that are used in a chain, see the
   *     https://docs.datastax.com/en/developer/java-driver/4.3/manual/query_builder/index.html
   */
  public abstract <StmtT extends OngoingWhereClause<StmtT>> StmtT apply(
      TableSchemaObject tableSchemaObject, StmtT ongoingWhereClause, List<Object> positionalValues);

  /**
   * All subClass tableFilter have access to this method. This method will check the tableSchema and
   * see if the filter column(path) has SAI index on it.
   *
   * @param tableSchemaObject tableSchemaObject
   * @return boolean to indicate if there is a SAI index on the column
   */
  public boolean hasSaiIndexOnColumn(TableSchemaObject tableSchemaObject) {

    // Check if the column has SAI index on it
    return tableSchemaObject.tableMetadata().getIndexes().values().stream()
        .anyMatch(index -> index.getTarget().equals(path));
  }

  /**
   * All subClass tableFilter have access to this method. This method will check the tableSchema and
   * see if the filter column(path) is on the primary key.
   *
   * @param tableSchemaObject tableSchemaObject
   * @return boolean to indicate if there is an index on the primary key
   */
  public boolean hasPrimaryKeyOnColumn(TableSchemaObject tableSchemaObject) {

    // Check if the column is a primary key (partition key or clustering column)
    boolean isPrimaryKey =
        tableSchemaObject.tableMetadata().getPartitionKey().stream()
                .anyMatch(column -> column.getName().equals(path))
            || tableSchemaObject.tableMetadata().getClusteringColumns().keySet().stream()
                .anyMatch(column -> column.getName().equals(path));
    return isPrimaryKey;
  }

  /**
   * Get the target columnMetaData if the column exists. Otherwise, throw TABLE_COLUMN_UNKNOWN Data
   * API exception.
   *
   * @param tableSchemaObject tableSchemaObject
   * @return Optional<ColumnMetadata>, columnMetadata
   */
  public ColumnMetadata getColumn(TableSchemaObject tableSchemaObject) {
    return tableSchemaObject.tableMetadata().getColumns().entrySet().stream()
        .filter(entry -> entry.getKey().asInternal().equals(path))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElseThrow(
            () ->
                ErrorCodeV1.TABLE_COLUMN_UNKNOWN.toApiException(
                    "No column with name '%s' found in table '%s'",
                    path, tableSchemaObject.tableMetadata().getName()));
  }

  /**
   * Method to check if current tableFilter needs ALLOW FILTERING on. All tableFilter will implement
   * this abstract method.
   *
   * <p>[use case 1] If there is no index on the target column, Data API will add ALLOW FILTERING
   * despite the performance unpredictability.
   *
   * <p>[user case 2] Some cql operators need ALLOW FILTERING on to perform the query, with or
   * without index. Then the tableFilter implementation should know itself to add ALLOW FILTERING or
   * not.
   *
   * <p>TODO, We may add more complex analyze in the future
   *
   * @param tableSchemaObject tableSchemaObject
   * @return boolean
   */
  public abstract TableFilterAnalyzedUsage analyze(TableSchemaObject tableSchemaObject);
}
