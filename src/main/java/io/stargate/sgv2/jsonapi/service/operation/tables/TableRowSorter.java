package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SortedRowAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.operation.query.InMemorySortComparator;
import io.stargate.sgv2.jsonapi.service.operation.query.RowSorter;
import java.util.Optional;

/**
 * Sorts rows in memory using the provided comparator.
 *
 * <p>Create using the {@link
 * io.stargate.sgv2.jsonapi.service.resolver.sort.TableMemorySortClauseResolver}
 */
public class TableRowSorter implements RowSorter {

  private final InMemorySortComparator comparator;
  private final SortedRowAccumulator rowAccumulator;

  private final int maxSortedRows;
  private int readPageSize;

  /**
   * @param comparator Comparator to use for sorting the rows we accumulate.
   * @param commandSkip Number of rows to skip, after the sorting.
   * @param commandLimit Number of rows to return, after the sorting.
   * @param maxSortedRows Maximum number of rows to sort.
   * @param readPageSize Number of rows to pull from the datbase in each page.
   */
  public TableRowSorter(
      InMemorySortComparator comparator,
      int commandSkip,
      int commandLimit,
      int maxSortedRows,
      int readPageSize) {
    this.comparator = comparator;
    this.maxSortedRows = maxSortedRows;
    this.readPageSize = readPageSize;

    // create the rows container now so we can get access to the number of rows it sorted later.
    this.rowAccumulator =
        new SortedRowAccumulator(
            new SortedRowAccumulator.RowSortSettings(commandSkip, commandLimit, maxSortedRows),
            comparator);
  }

  @Override
  public Optional<Integer> sortedRowCount() {
    return Optional.of(rowAccumulator.getSortedRowsCount());
  }

  @Override
  public Select addToSelect(Select select) {
    return select.columnsIds(comparator.orderingColumns());
  }

  @Override
  public CQLOptions<Select> updateCqlOptions(CQLOptions<Select> cqlOptions) {
    cqlOptions.addBuilderOption(CQLOption.ForSelect.limit(maxSortedRows));
    cqlOptions.addStatementOption(CQLOption.ForStatement.pageSize(readPageSize));
    return cqlOptions;
  }

  @Override
  public CqlPagingState updatePagingState(CqlPagingState pagingState) {
    // currently cannot support paging when doing in memory sorting
    return CqlPagingState.EMPTY;
  }

  @Override
  public CqlPagingState buildPagingState(AsyncResultSet resultSet) {
    // currently cannot support paging when doing in memory sorting
    return CqlPagingState.EMPTY;
  }

  @Override
  public Uni<AsyncResultSet> executeRead(
      CommandQueryExecutor queryExecutor, SimpleStatement statement) {
    return queryExecutor.executeReadAllPages(statement, rowAccumulator);
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("TableRowSorter{")
        .append("comparator=")
        .append(comparator)
        .append(", rowAccumulator=")
        .append(rowAccumulator)
        .append(", maxSortedRows=")
        .append(maxSortedRows)
        .append(", readPageSize=")
        .append(readPageSize)
        .append('}')
        .toString();
  }
}
