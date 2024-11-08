package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.Objects;

/**
 * Wraps a {@link Row} to cache values read from it and avoid repeatedly deserializing data when
 * comparing values for sorting.
 *
 * <p>The driver passes the bytes through the codec every time you call {@link Row#getObject(int)}.
 */
public class ValueCachingRow {

  private final Row row;

  // array of values we have read and so do not need to decode again, using array so we can cache
  // using the column index as an int
  private final Object[] valueCache;
  // flag to indicate if the value is already read, cannot rely on null check as null is a valid
  // value
  private final boolean[] cacheFilled;

  public ValueCachingRow(Row row) {
    this.row = Objects.requireNonNull(row, "row must not be null");
    this.valueCache = new Object[row.getColumnDefinitions().size()];
    this.cacheFilled = new boolean[row.getColumnDefinitions().size()];
  }

  public Row getRow() {
    return row;
  }

  /**
   * Get the value of the column at the given index, caching the value if it has not been read
   * before.
   *
   * @param columnIndex The index of the column to read, the caller should make sure this is valid
   *     by using {@link Row#firstIndexOf(CqlIdentifier)}
   * @return The value of the column
   */
  public Object getObject(int columnIndex) {
    if (cacheFilled[columnIndex]) {
      return valueCache[columnIndex];
    }

    cacheFilled[columnIndex] = true;
    return valueCache[columnIndex] = row.getObject(columnIndex);
  }
}
