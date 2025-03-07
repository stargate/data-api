package io.stargate.sgv2.jsonapi.service.operation.query;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

/**
 * Interface that describes how a where clause behaves.
 *
 * <p>Any behaviour about a where clause should be described in this interface so that decisions
 * about warnings etc are (as much as possible) based on the interface and not understanding what is
 * in the where clause.
 *
 * <p>Implementations should return a result based on the configuration of the specific where
 * clause, not the general case.
 */
public interface WhereBehaviour {

  /**
   * Called to determine if the where clause will select a single partition.
   *
   * <p>This is important when making decisions about the use of cluster ordering or in memory
   * sorting.
   *
   * @param tableSchemaObject The table the where clause is being applied to.
   * @return Implementations should return true if the where clause will select a single partition.
   *     For example when all partition keys are restricted by `=`, or there is an `IN` restriction
   *     that has a single value.
   */
  boolean selectsSinglePartition(TableSchemaObject tableSchemaObject);
}
