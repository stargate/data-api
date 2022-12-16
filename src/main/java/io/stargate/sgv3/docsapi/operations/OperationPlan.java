package io.stargate.sgv3.docsapi.operations;

/**
 * Describes how the operation will run, such as if it can do full DB pushdown if it needs to filter
 * etc.
 *
 * <p>This may be extended to calculate a cost for each operation, e.g. find by ID cost 1, find by 2
 * fields = 4. However, that only makes sense if we come up with multiple {@link Operation}s for a
 * single {@link Command}.
 *
 * <p>For now this can be used by the {@link OperationExecutor} to decide what resources to use and
 * how to schedule running a {@link Operation}. E.g. if there is client side sorting run that in a
 * diff pool (?) to a quick DB pushdown operaton.
 */
public class OperationPlan {

  /**
   * If true this operation is fully pushed down to the DB and does not use any client side
   * filtering or sorting etc.
   */
  public final boolean isPushdown;

  public OperationPlan(boolean isPushdown) {
    this.isPushdown = isPushdown;
  }
}
