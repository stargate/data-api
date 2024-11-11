package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.MinMaxPriorityQueue;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * RowAccumulator implementation which sorts the added rows as they are added, and trims the memory usage
 * to only keep skip + limit rows in memory.
 */
public class SortedRowAccumulator implements RowAccumulator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedRowAccumulator.class);

  private final MinMaxPriorityQueue<ValueCachingRow> rows;
  private final RowSortSettings rowSortSettings;
  private final int maxSortWindowSize;

  private int sortedRowsCount = 0;

  public SortedRowAccumulator(
      RowSortSettings rowSortSettings, Comparator<ValueCachingRow> comparator) {

    this.rowSortSettings =
        Objects.requireNonNull(rowSortSettings, "rowSortSettings must not be null");
    this.maxSortWindowSize = rowSortSettings.skip() + rowSortSettings.returnLimit();
    this.rows = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(maxSortWindowSize).create();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "SortedRowAccumulator() created with rowSortSettings={}, comparator={} and maxSortWindowSize={}",
          rowSortSettings,
          comparator,
          maxSortWindowSize);
    }
  }

  /**
   * Gets the total number of rows that were sorted, that is all the rows that were read from the
   * database.
   *
   * @return The total number of rows that were sorted.
   */
  public int getSortedRowsCount() {
    return sortedRowsCount;
  }

  @Override
  public List<Row> getPage() {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "getPage() building page from sorted rows sortedRowsCount={}, rows.size={}, maxSortWindowSize={}, rowSortSettings={}",
          sortedRowsCount,
          rows.size(),
          maxSortWindowSize,
          rowSortSettings);
    }

    // If the skip is 0 and the return limit is the max value, return all the rows
    if (rowSortSettings.skip() == 0 && rowSortSettings.returnLimit() == Integer.MAX_VALUE) {
      LOGGER.debug("getPage() returning all rows because skip is 0 and returnLimit is max");
      return rows.stream().map(ValueCachingRow::getRow).toList();
    }
    // begin value to read from the sorted list
    int begin = rowSortSettings.skip();

    // If the begin index is >= sorted list size, return empty response
    if (begin >= rows.size()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "getPage() returning empty list because skip={} which is >= rows.size={}",
            begin,
            rows.size());
      }
      return Collections.emptyList();
    }

    // Last index to which we need to read
    int end = Math.min(rowSortSettings.skip() + rowSortSettings.returnLimit(), rows.size());
    int discarded = 0;
    // Create a sublist of the required rage
    List<Row> subList = new ArrayList<>(rowSortSettings.returnLimit());
    for (int i = 0; i < end; i++) {
      var cachedRow = rows.poll();
      if (i >= begin) {
        subList.add(cachedRow.getRow());
      } else {
        discarded++;
      }
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "getPage() returning subList of size={} with discarded={} rows",
          subList.size(),
          discarded);
    }
    return subList;
  }

  @Override
  public boolean accumulate(Row row) {
    sortedRowsCount++;
    if (sortedRowsCount >= rowSortSettings.maxSortedRows()) {
      return false;
    }
    rows.add(new ValueCachingRow(row));
    return true;
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("SortedRowAccumulator{")
        .append("rows.count=")
        .append(rows.size())
        .append(", rowSortSettings=")
        .append(rowSortSettings)
        .append(", sortedRowsCount=")
        .append(sortedRowsCount)
        .append('}')
        .toString();
  }

  /**
   * Settings for the row sorting
   *
   * @param skip The number of rows to skip after the sorting has been done, this is the beginning
   *     to the results page.
   * @param returnLimit The number of rows to return after the sorting has been done, this is the
   *     end of the results page.
   * @param maxSortedRows The maximum number of rows to sort, this is how many rows we will pull
   *     from the database to sort.
   */
  public record RowSortSettings(int skip, int returnLimit, int maxSortedRows) {}
}
