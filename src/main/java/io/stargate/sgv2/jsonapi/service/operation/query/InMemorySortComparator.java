package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.data.IdentifierIndex;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.ValueCachingRow;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/*
 * Comparator implementation to do in-memory sorting of rows read.
 */
public class InMemorySortComparator implements Comparator<ValueCachingRow> {

  private final List<SortByTerm> sortTerms;

  public InMemorySortComparator(List<SortByTerm> sortTerms) {
    this.sortTerms = Objects.requireNonNull(sortTerms, "sortBy must not be null");

    // sanity check, we should not be created if there is nothing to do
    if (sortTerms.isEmpty()) {
      throw new IllegalArgumentException("No sort columns provided");
    }
  }

  /**
   * Returns the list of the columns that are being sorted on, in the order they are sorted.
   *
   * @return The list of columns being sorted on
   */
  public List<CqlIdentifier> orderingColumns() {
    return sortTerms.stream().map(sortByTerm -> sortByTerm.apiColumnDef.name()).toList();
  }

  @Override
  public int compare(ValueCachingRow o1, ValueCachingRow o2) {
    for (SortByTerm sortColumn : sortTerms) {
      int compareValue = compareValues(o1, o2, sortColumn);
      if (compareValue != 0) {
        return compareValue;
      }
    }
    return 0; // All compared values are equal
  }

  private static int compareValues(ValueCachingRow o1, ValueCachingRow o2, SortByTerm sortByTerm) {
    final Object value1 = sortByTerm.getColumnValue(o1);
    final Object value2 = sortByTerm.getColumnValue(o2);

    // Handle nulls explicitly to avoid NullPointerExceptions
    if (value1 == null && value2 == null) {
      return 0;
    }
    if (value1 == null) {
      return sortByTerm.ascending ? -1 : 1;
    }
    if (value2 == null) {
      return sortByTerm.ascending ? 1 : -1;
    }

    int compare;
    if (value1 instanceof Comparable) {
      compare = uncheckedToComparable(value1).compareTo(value2);
    } else {
      throw new IllegalArgumentException("value1 is not comparable: " + value1 + ", " + value2);
    }

    return sortByTerm.ascending ? compare : (-1 * compare);
  }

  @SuppressWarnings("unchecked")
  private static Comparable<Object> uncheckedToComparable(Object object) {
    return (Comparable<Object>) object;
  }

  /** The sort column and option to be sorted and the direction ascending/descending. */
  public static class SortByTerm {

    final ApiColumnDef apiColumnDef;
    final boolean ascending;

    // using int not Integer to avoid boxing/unboxing because driver wants int
    private int columnIndex = Integer.MIN_VALUE;

    public SortByTerm(ApiColumnDef apiColumnDef, boolean ascending) {
      this.apiColumnDef = Objects.requireNonNull(apiColumnDef, "apiColumnDef must not be null");
      this.ascending = ascending;
    }

    /**
     * Get the value of the column this term is sorting on, from the row.
     *
     * <p>Pushing down here so we can cache the column index and avoid looking up the column name in
     * the row, see the {@link IdentifierIndex} in the driver. Getting the column index from the
     * name is quick, but if we need to do it thousands of times we should do it once.
     *
     * @param row The row to get the column value from
     * @return The value of the column
     */
    Object getColumnValue(ValueCachingRow row) {
      if (columnIndex < 0) {
        // using the column index to avoid the need to look up the column name in the row
        // will throw if the column is not found, that is OK should not happen
        columnIndex = row.getRow().firstIndexOf(apiColumnDef.name());
      }
      return row.getObject(columnIndex);
    }
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("InMemorySortComparator{")
        .append("sortTerms=")
        .append(sortTerms)
        .append('}')
        .toString();
  }
}
