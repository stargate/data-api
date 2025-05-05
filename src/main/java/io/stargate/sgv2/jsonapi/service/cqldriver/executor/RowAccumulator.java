package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.cql.Row;
import java.util.List;

/**
 * Interface for collecting rows read from the driver.
 *
 * <p>This exists so that different implementations can be used when we need to spool all the rows
 * from a table for in memory sorting. In all cases the role of implementations to collect {@link
 * Row}'s read from the driver and provide a single page of rows to the caller.
 */
public interface RowAccumulator {

  /**
   * Returns the page of rows the accumulator has collected.
   *
   * @return A list of {@link Row}, successive calls to the method should return the same rows.
   *     Though it should only be called once, so implementations do not need to optimize for
   *     multiple calls.
   */
  List<Row> getPage();

  /**
   * Adds a row to the container.
   *
   * @return true if the row was added, false otherwise. If false is returned the caller should stop
   *     adding rows as a limit has been reached.
   */
  boolean accumulate(Row row);
}
