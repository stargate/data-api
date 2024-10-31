package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.cql.Row;
import java.util.ArrayList;
import java.util.List;

public interface RowsContainer {
  List<Row> getRequiredPage();

  boolean add(Row row);

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
