package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.Row;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import java.util.Comparator;
import java.util.List;

public class TableInmemorySortClause implements Comparator<Row> {

  private final List<OrderBy> orderBy;

  public TableInmemorySortClause(List<OrderBy> orderBy) {
    this.orderBy = orderBy;
  }

  public static TableInmemorySortClause DEFAULT = new TableInmemorySortClause(List.of());

  @Override
  public int compare(Row o1, Row o2) {
    for (OrderBy sortColumn : orderBy) {
      int compareValue = compareValues(o1, o2, sortColumn.apiColumnDef(), sortColumn.ascending());
      if (compareValue != 0) {
        return compareValue;
      }
    }
    return 0; // All compared values are equal
  }

  private int compareValues(Row o1, Row o2, ApiColumnDef apiColumnDef, boolean ascending) {
    final Object value1 = o1.getObject(apiColumnDef.name());
    final Object value2 = o2.getObject(apiColumnDef.name());

    // Handle nulls explicitly to avoid NullPointerExceptions
    if (value1 == null && value2 == null) return 0;
    if (value1 == null) return ascending ? -1 : 1;
    if (value2 == null) return ascending ? 1 : -1;

    int compare;
    if (value1 instanceof Comparable && value2 instanceof Comparable) {
      compare = ((Comparable<Object>) value1).compareTo(value2);
    } else {
      throw new IllegalArgumentException("Values are not comparable: " + value1 + ", " + value2);
    }

    return ascending ? compare : (-1 * compare);
  }

  /**
   * Represents sort column and option to be sorted ascending/descending.
   *
   * @param apiColumnDef
   * @param ascending
   */
  public record OrderBy(ApiColumnDef apiColumnDef, boolean ascending) {}
}
