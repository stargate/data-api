package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.time.Duration;
import java.util.Objects;

/**
 * A policy for retrying in a {@link BaseTask}, this is used at the task level and so it in addition
 * to any retrying a driver (like cassandra driver) may do.
 *
 * <p>If the attempt does not want to retry then it should use {@link TaskRetryPolicy#NO_RETRY}.
 *
 * <p>To implement a custom retry policy, subclass this class and override {@link
 * #shouldRetry(Throwable)}.
 */
public class TaskRetryPolicy implements Recordable {

  /** Re-usable policy to not retry. */
  public static final TaskRetryPolicy NO_RETRY = new TaskRetryPolicy();

  private final int maxRetries;
  private final Duration delay;

  protected TaskRetryPolicy() {
    this(1, Duration.ofMillis(1));
  }

  /**
   * Construct a new retry policy with the provided max retries and delay.
   *
   * @param maxRetries the number of retries after the initial attempt, must be >= 1
   * @param delay the delay between retries, must not be <code>null</code>
   */
  public TaskRetryPolicy(int maxRetries, Duration delay) {
    // This is a requirement of UniRetryAtMost that is it >= 1, however UniRetry.atMost() says it
    // must be >= 0
    if (maxRetries < 1) {
      throw new IllegalArgumentException("maxRetries must be >= 1");
    }
    this.maxRetries = maxRetries;
    this.delay = Objects.requireNonNull(delay, "delay cannot be null");
  }

  public int maxRetries() {
    return maxRetries;
  }

  public Duration delay() {
    return delay;
  }

  /**
   * Called by the {@link BaseTask} to decide if the task should be retried.
   *
   * <p>The policy does not need to keep track of the retry counts, this method is only called if
   * more retries are allowed according to the {@link #maxRetries()}.
   *
   * @param throwable The exception raised when the task tried to fetch the results. This will be
   *     after any driver level re-trying and before mapping to a user exception.
   * @return <code>True</code> if the attempt should be retried, defaults is false because this is
   *     at the whole task level not the specific db statement
   */
  public boolean shouldRetry(Throwable throwable) {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    return dataRecorder.append("maxRetries", maxRetries).append("delay", delay);

  }
}
