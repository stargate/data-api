package io.stargate.sgv2.jsonapi.service.operation.query;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.IndexUsage;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.List;

/**
 * A {@link DBFilterBase} that is applied to a table (i.e. not a Collection) to filter the rows to
 * read.
 */
public abstract class TableFilter extends DBFilterBase implements FilterBehaviour {

  private final CqlIdentifier column;
  private final FilterBehaviour filterBehaviour;

  /**
   * Create a new filter for the given column.
   *
   * @param path user provided path for the column we are filtering on. The name "path" is a
   *     hangover from collections.
   * @param filterBehaviour Optional object to pass through calls to the {@link FilterBehaviour}
   *     interface. If subclasses have an object representing an operator the operator can implement
   *     the interface. Otherwise, the subclass <b>must</b> implement the methods for {@link
   *     FilterBehaviour} itself, the default in this class will be to throw a {@link
   *     UnsupportedOperationException}.
   */
  protected TableFilter(String path, FilterBehaviour filterBehaviour) {
    super(path, IndexUsage.NO_OP);
    this.column = cqlIdentifierFromUserInput(path);
    this.filterBehaviour = filterBehaviour == null ? UNSUPPORTED_BEHAVIOUR : filterBehaviour;
  }

  /**
   * Tests if this filter matches the given column.
   *
   * @param column The column to test, may be <code>null</code>
   * @return true if the filter matches the column, false otherwise or if the <code>column</code> is
   *     null
   */
  public boolean isFor(CqlIdentifier column) {
    return this.column.equals(column);
  }

  @Override
  public boolean filterIsExactMatch() {
    return filterBehaviour.filterIsExactMatch();
  }

  @Override
  public boolean filterIsSlice() {
    return filterBehaviour.filterIsSlice();
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

  /** Default "NO OP" implementation to keep the code above cleaner. */
  private static final FilterBehaviour UNSUPPORTED_BEHAVIOUR =
      new FilterBehaviour() {
        @Override
        public boolean filterIsExactMatch() {
          throw new UnsupportedOperationException("isExactMatch not supported");
        }

        @Override
        public boolean filterIsSlice() {
          throw new UnsupportedOperationException("isSlice not supported");
        }
      };
}
