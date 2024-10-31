package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.function.Function;

/**
 * Interface for a class that can add OrderBY to a CQL query built using the Java Driver Query
 * Builder. This includes ANN sorting.
 *
 * <p>This is the ORDER BY part below:
 *
 * <pre>
 * SELECT
 *  column1, column2
 * FROM
 *  MyTable
 * WHERE
 *  columnName = B70DE1D0-9908-4AE3-BE34-5573E5B09F14;
 * ORDER BY column1 ASC, column2 DESC;
 * </pre>
 */
public interface OrderByCqlClause extends Function<Select, Select>, CQLClause {

  // No Op implementation just returns the select, use this when no order by is needed
  OrderByCqlClause NO_OP = select -> select;

  /**
   * If true, this means that the query will need to be sorted in memory after the query is
   * executed.
   *
   * @return true if the query needs to be sorted in memory
   */
  default boolean inMemorySortNeeded() {
    return true;
  }
}
