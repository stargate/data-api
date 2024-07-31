package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.IndexUsage;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.List;

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
}
