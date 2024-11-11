package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import java.util.Optional;

/**
 * Interface for how we sort rows read from the driver in memory, for situations where the DB cannot
 * sort the results for us.
 *
 * <p>When no sorting is required, the {@link #NO_OP} instance should be used.
 */
public interface RowSorter {

  /**
   * A row sorter that does no sorting, this is the path when we either do not have a sort clause or
   * the sort can be done by the database using CQL ORDER BY
   */
  RowSorter NO_OP =
      new RowSorter() {
        @Override
        public Select addToSelect(Select select) {
          return select;
        }

        @Override
        public CQLOptions<Select> updateCqlOptions(CQLOptions<Select> cqlOptions) {
          return cqlOptions;
        }

        @Override
        public CqlPagingState updatePagingState(CqlPagingState pagingState) {
          return pagingState;
        }

        @Override
        public CqlPagingState buildPagingState(AsyncResultSet resultSet) {
          return CqlPagingState.from(resultSet);
        }

        @Override
        public Uni<AsyncResultSet> executeRead(
            CommandQueryExecutor queryExecutor, SimpleStatement statement) {
          return queryExecutor.executeRead(statement);
        }

        @Override
        public Optional<Integer> sortedRowCount() {
          return Optional.empty();
        }
      };

  /**
   * Called so the sorter can add any additional columns it needs to read to the select clause.
   *
   * <p>The sorter is not responsible for ensuring these are not duplicates, or that the columns are
   * not returned in the result.
   *
   * <p>May be called multiple times if an attempt is retried, but in each case the select will be a
   * new instance.
   *
   * @param select Driver {@link Select} query builder to add columns to.
   * @return Updated {@link Select} query builder.
   */
  Select addToSelect(Select select);

  /**
   * Called when the query is being built so that the sorter can add or override any options used
   * when building the select statement. This is where it can change the limit or page size.
   *
   * <p>May be called multiple times if an attempt is retried, in each case the <code>cqlOptions
   * </code> will be the same object. Implementations should just update the object the same each
   * time.
   *
   * @param cqlOptions The current {@link CQLOptions} object.
   * @return Updated {@link CQLOptions} object.
   */
  CQLOptions<Select> updateCqlOptions(CQLOptions<Select> cqlOptions);

  /**
   * Called when the query is being build, so that the sorter can update the paging state that will
   * be used for when the statement is sent to the db.
   *
   * @param pagingState The current paging state, never null.
   * @return Updated paging state, never null. Use {@link CqlPagingState#EMPTY} if no paging state
   *     is needed.
   */
  CqlPagingState updatePagingState(CqlPagingState pagingState);

  /**
   * Called after the query has executed to build the paging state that will be returned to in the
   * command result.
   *
   * @param resultSet The result set from the query execution.
   * @return Updated paging state, never null. Use {@link CqlPagingState#EMPTY} if no paging state
   *     is needed.
   */
  CqlPagingState buildPagingState(AsyncResultSet resultSet);

  /**
   * Called to execute the read of the statement, the sorter is responsbile for calling the
   * appropriate method on the {@link CommandQueryExecutor}, e.g. to read one page or read all pages
   * from the database and sort them.
   *
   * @param queryExecutor The query executor to use to execute the statement.
   * @param statement The statement to execute.
   * @return The result of the query execution.
   */
  Uni<AsyncResultSet> executeRead(CommandQueryExecutor queryExecutor, SimpleStatement statement);

  /**
   * Called to get the number of rows that were sorted by the sorter, this is used to report the
   * number of rows that were sorted in the command result.
   *
   * @return The number of rows sorted by the sorter, or empty if the sorter did not attempt to sort
   *     any rows. Note: if it wanted to sort rows, but actually sorted 0 it should return 0.
   */
  Optional<Integer> sortedRowCount();
}
