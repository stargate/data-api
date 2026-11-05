package io.stargate.sgv2.jsonapi.testbench.lifecycle;

import io.stargate.sgv2.jsonapi.testbench.testspec.Job;

/**
 * Defines the stages a {link @Job} may go through as it is executing.
 *
 * <p>It's limited now to just the one method, may never get bigger, but there are a couple of
 * implementations so extracted.
 */
public interface JobLifeCycle {

  /**
   * Called to update the Job with any information it needs to run against the particular target.
   *
   * <p>Typcially this means setting or updating the {@link Job#variables()} for things like a
   * default keyspace name or implementing naming restrictions.
   *
   * <p>NOTE: The target or Backend must set the <code>KEYSPACE_NAME</code> job variable
   *
   * @param job The job to update
   */
  void updateJobForTarget(Job job);
}
