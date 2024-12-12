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
   * Describes if the Order By fully covers the sort, skip, and limit from the command.
   *
   * <p>NOTE: Default is false, implementations must override this if they want to prevent in memory
   * sorting
   *
   * @return true if the Order By fully covers the command, false otherwise meaning there should be
   *     in memory sorting, skip, and limit.
   */
  default boolean fullyCoversCommand() {
    return false;
  }

  /**
   * Called to get the default limit for the query when using this order by.
   *
   * <p>ANN order by has a different default that regular queries to limit the number of rows in
   * consideration.
   *
   * @return Default returns {@link Integer#MAX_VALUE} so queries can read all the rows in the
   *     table.
   */
  default Integer getDefaultLimit() {
    return Integer.MAX_VALUE;
  }
}
