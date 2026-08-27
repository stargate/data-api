package io.stargate.sgv2.jsonapi.testbench.messaging;

import io.restassured.response.Response;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestRunEnv;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** --- */
interface APIRetryPolicy {

  Logger LOGGER = LoggerFactory.getLogger(APIRetryPolicy.class);

  String RETRY_MATCH_STRING_DELIM = "\t";
  String ENV_RETRY_MATCH_STRING = "RETRY_MATCH_STRING";

  String DEFAULT_MAX_ATTEMPTS = "5";
  String ENV_RETRY_MAX_ATTEMPTS = "END_RETRY_MAX_ATTEMPTS";

  String DEFAULT_BASE_SLEEP_MS = "5000";
  String ENV_RETRY_BASE_SLEEP_MS = "RETRY_BASE_SLEEP_MS";

  String DEFAULT_JITTER_MS = "2000";
  String ENV_RETRY_JITTER_MS = "RETRY_JITTER_MS";

  /**
   * Retries are driven by the presence of the following keys in the {@link TestRunEnv}. When a
   * retry is performed the response for that request is lost, the response of the call to <code>
   * execute</code> will be the response from the last attempt.
   *
   * <ul>
   *   <li>{@link #ENV_RETRY_MATCH_STRING} Enables retry if present and non blank string, treated as
   *       a tab {@link #RETRY_MATCH_STRING_DELIM} delimtered string. If any of the tokens is
   *       present in the response body the request is retried. Example: <code>
   *       "EMBEDDING_PROVIDER_RATE_LIMITED\tEMBEDDING_PROVIDER_TIMEOUT"</code>
   *   <li>{@link #ENV_RETRY_MAX_ATTEMPTS} total number of attempts to make before returning, must
   *       be above 1, default is {@link #ENV_RETRY_MAX_ATTEMPTS}
   *   <li>{@link #ENV_RETRY_BASE_SLEEP_MS} base number of milliseconds between attempts, this is
   *       multiplied by 2 ^ attempt, so base of 5000 means sleep of 5, 10, 20 seconds
   *   <li>{@link #ENV_RETRY_JITTER_MS} Upper bound on the random number of milliseconds to add to
   *       each attempt.
   * </ul>
   */
  static APIRetryPolicy createRetryPolicy(TestRunEnv testRunEnv) {

    var retryMatch = testRunEnv.get(ENV_RETRY_MATCH_STRING);
    if (retryMatch == null || retryMatch.isBlank()) {
      return new NoAPIRetryPolicy();
    }

    var maxAttempts =
        Integer.parseInt(testRunEnv.get(ENV_RETRY_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS));
    var baseSleepMs =
        Long.parseLong(testRunEnv.get(ENV_RETRY_BASE_SLEEP_MS, DEFAULT_BASE_SLEEP_MS));
    var jitterMs = Long.parseLong(testRunEnv.get(ENV_RETRY_JITTER_MS, DEFAULT_JITTER_MS));

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "createRetryPolicy() - retryMatch={}, maxAttempts={}, baseSleepMs={}, jitterMs={}",
          retryMatch,
          maxAttempts,
          baseSleepMs,
          jitterMs);
    }

    return new ConfiguredAPIRetryPolicy(
        List.of(retryMatch.split(RETRY_MATCH_STRING_DELIM)), maxAttempts, baseSleepMs, jitterMs);
  }

  /** Call to get the first decision, this is used to count the number of attempts. */
  default RetryDecision firstAttempt() {
    return RetryDecision.FIRST_ATTEMPT;
  }

  /**
   * Call to decide if we should retry or not, implementations will sleep if needed.
   *
   * @param lastDecision The last decision made, used to count the number of attempts
   * @param response The response from the last attempt
   * @return A decision to retry or not, along with the attempt count, use for the next call to this
   *     method.
   */
  RetryDecision decide(RetryDecision lastDecision, Response response);

  /**
   * A decision to retry or not, along with the attempt count. This is where we count the number of
   * attempts.
   */
  record RetryDecision(boolean retry, int attempt) {

    static final RetryDecision FIRST_ATTEMPT = new RetryDecision(true, 1);

    RetryDecision stopAttempts() {
      // do not increase the attempt count, we won't make another.
      return new RetryDecision(false, attempt);
    }
  }

  /** A retry policy that never retries, makes a single attempt. */
  record NoAPIRetryPolicy() implements APIRetryPolicy {
    private static final RetryDecision NO_RETRY = new RetryDecision(false, 1);

    @Override
    public RetryDecision decide(RetryDecision lastDecision, Response response) {
      return NO_RETRY;
    }
  }

  /**
   * Configurable retry policy with customizable retry conditions, maximum attempts, and backoff
   * strategy.
   */
  record ConfiguredAPIRetryPolicy(
      List<String> retryMatch, int maxAttempts, long baseSleepMs, long jitterMs)
      implements APIRetryPolicy {

    public ConfiguredAPIRetryPolicy {

      if (retryMatch == null || retryMatch.isEmpty()) {
        throw new IllegalArgumentException("retryMatch is null or empty");
      }
      if (maxAttempts < 2) {
        throw new IllegalArgumentException(
            "maxAttempts must be greater than 1, got: " + maxAttempts);
      }
    }

    @Override
    public RetryDecision decide(RetryDecision lastDecision, Response response) {

      if (lastDecision.attempt == maxAttempts) {
        return lastDecision.stopAttempts();
      }

      var body = response.body().asString();

      for (var match : retryMatch) {
        if (body.contains(match)) {
          // Service has a concurrency limit and retrying runners can collide regardless of jitter.
          // Base backoff is long enough to wait out an in-flight request (5s, 10s, 20s...),
          // plus a small random offset to avoid re-synchronising after the wait.
          long baseMs = (long) (5000 * Math.pow(2, lastDecision.attempt));
          long jitterMs = ThreadLocalRandom.current().nextLong(2000);
          long sleepMs = baseMs + jitterMs;

          LOGGER.info(
              "executeRequest() - Retrying, found retry string in response. match={}, sleepMs={} ms, lastDecision.attempt={}",
              match,
              sleepMs,
              lastDecision.attempt);
          try {
            Thread.sleep(sleepMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry sleep", e);
          }

          // try again, increase the attempt counter.
          return new RetryDecision(true, lastDecision.attempt + 1);
        }
      }
      return lastDecision.stopAttempts();
    }
  }
}
