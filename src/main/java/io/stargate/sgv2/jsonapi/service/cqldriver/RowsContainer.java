package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.cql.Row;
import java.util.ArrayList;
import java.util.List;

/** Interface representing a container for rows, providing methods to retrieve and add rows. */
public interface RowsContainer {
  /** Returns the required number of rows based on user options. */
  List<Row> getRequiredPage();

  /*
   * Adds a row to the container. Returns true if the row was added successfully, false otherwise.
   */
  boolean add(Row row);

  /**
   * Indicates whether to read all pages from the database.
   *
   * @return true if all pages should be read, false otherwise.
   */
  default boolean readAllPages() {
    return false;
  }

  static RowsContainer defaultRowsContainer() {
    return new RowsContainer() {
      private final List<Row> rows = new ArrayList<>();

      @Override
      public List<Row> getRequiredPage() {
        return new ArrayList<>(rows);
      }

      @Override
      public boolean add(Row row) {
        return rows.add(row);
      }
    };
  }
}
