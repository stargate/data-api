package io.stargate.sgv2.jsonapi.util;

import com.github.benmanes.caffeine.cache.Ticker;
import io.stargate.sgv2.jsonapi.TestConstants;

import java.time.Duration;

public class CacheTestsBase {

  protected final TestConstants TEST_CONSTANTS = new TestConstants();

  // Note - using the FakeTicker to control the time for testing, so we dont actally wait 10 seconds
  protected final String CACHE_NAME = "test-cache" + TEST_CONSTANTS.CORRELATION_ID;
  protected final Duration LONG_TTL = Duration.ofSeconds(10);

  protected final int CACHE_MAX_SIZE = 10;
  protected final Duration SHORT_TTL = Duration.ofSeconds(5);

  /** {@link Ticker} for the cache so we can control the time for testing differentiated TTL */
  public static class FakeTicker implements Ticker {

    private long nanos = 0;

    @Override
    public long read() {
      return nanos;
    }

    public void advance(Duration duration) {
      nanos += duration.toNanos();
    }
  }
}
