package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.MinMaxPriorityQueue;
import io.stargate.sgv2.jsonapi.service.operation.query.InMemorySortOption;
import io.stargate.sgv2.jsonapi.service.operation.tables.ValueCachedRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/*
 * RowsContainer implementation which sorts the added rows
 */
public class SortedRowsContainer implements RowsContainer {
  private final MinMaxPriorityQueue<ValueCachedRow> rows;
  private final InMemorySortOption inMemorySortOption;
  private int counter;

  public SortedRowsContainer(
      InMemorySortOption inMemorySortOption, Comparator<ValueCachedRow> comparator) {
    this.inMemorySortOption = inMemorySortOption;
    this.rows =
        MinMaxPriorityQueue.orderedBy(comparator)
            .maximumSize(inMemorySortOption.skip() + inMemorySortOption.returnLimit())
            .create();
  }

  @Override
  public List<Row> getRequiredPage() {
    if (inMemorySortOption.skip() == 0 && inMemorySortOption.returnLimit() == Integer.MAX_VALUE) {
      return rows.stream().map(ValueCachedRow::getRow).toList();
    }
    // begin value to read from the sorted list
    int begin = inMemorySortOption.skip();

    // If the begin index is >= sorted list size, return empty response
    if (begin >= rows.size()) return Collections.emptyList();
    // Last index to which we need to read
    int end = Math.min(inMemorySortOption.skip() + inMemorySortOption.returnLimit(), rows.size());
    // Create a sublist of the required rage

    List<Row> subList = new ArrayList<>(inMemorySortOption.returnLimit());
    int i = 0;
    while (i < end) {
      var cachedRow = rows.poll();
      if (i >= begin) {
        subList.add(cachedRow.getRow());
      }
      i++;
    }
    return subList;
  }

  @Override
  public boolean add(Row row) {
    counter++;
    if (counter >= inMemorySortOption.errorLimit()) {
      return false;
    }
    rows.add(new ValueCachedRow(row));
    return true;
  }

  @Override
  public boolean readAllPages() {
    return true;
  }
}
