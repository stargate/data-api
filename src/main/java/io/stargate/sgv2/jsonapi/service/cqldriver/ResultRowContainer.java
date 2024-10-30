package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.MinMaxPriorityQueue;
import io.stargate.sgv2.jsonapi.service.operation.query.InMemorySortOption;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInmemorySortClause;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class ResultRowContainer implements Collection<Row> {
  private final MinMaxPriorityQueue<Row> rows;
  private final InMemorySortOption inMemorySortOption;
  private int counter;

  /*
   * Constructor to initialize the SortedRowsAsyncResultSet with skip, limit, errorLimit and
   * comparator for sorting rows.
   * @param skip - number of rows to skip
   * @param limit - maximum number of rows to return
   * @param errorLimit - If more rows than the errorLimit is read, error out
   * @param comparator - comparator for sorting rows
   */
  public ResultRowContainer(InMemorySortOption inMemorySortOption, Comparator<Row> comparator) {
    this.inMemorySortOption = inMemorySortOption;
    this.rows =
        MinMaxPriorityQueue.orderedBy(comparator)
            .maximumSize(inMemorySortOption.skip() + inMemorySortOption.returnLimit())
            .create();
  }

  public static ResultRowContainer DEFAULT =
      new ResultRowContainer(InMemorySortOption.DEFAULT, TableInmemorySortClause.DEFAULT);

  @Override
  public int size() {
    return rows.size();
  }

  @Override
  public boolean isEmpty() {
    return rows.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Iterator<Row> iterator() {
    return getRequiredPage().iterator();
  }

  public List<Row> getRequiredPage() {
    if (inMemorySortOption.skip() == 0 && inMemorySortOption.returnLimit() == Integer.MAX_VALUE) {
      return new ArrayList<>(rows);
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
      Row row = rows.poll();
      if (i >= begin) {
        subList.add(row);
      }
      i++;
    }
    return subList;
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean add(Row row) {
    counter++;
    if (counter >= inMemorySortOption.errorLimit()) {
      return false;
    }
    rows.add(row);
    return true;
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean addAll(Collection<? extends Row> c) {
    for (Row row : c) {
      if (!add(row)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
