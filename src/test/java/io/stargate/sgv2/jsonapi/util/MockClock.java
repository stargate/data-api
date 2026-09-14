package io.stargate.sgv2.jsonapi.util;

import java.time.*;
import java.util.concurrent.atomic.AtomicReference;

/** Implementation of the Java Clock that can be used to control time for time dependant tests. */
public class MockClock extends Clock {
  private final Instant startedAt;
  private final AtomicReference<Instant> now;
  private final ZoneId zone;

  public MockClock() {
    this(Instant.now(), ZoneId.systemDefault());
  }

  public MockClock(MockClock other) {
    this(other.startedAt, other.zone);
  }

  private MockClock(Instant now, ZoneId zone) {
    this.now = new AtomicReference<>(now);
    this.startedAt = now;
    this.zone = zone;
  }

  public Instant startedAt() {
    return startedAt;
  }

  public MockClock nextSecond() {
    return addSeconds(1);
  }

  public MockClock addSeconds(int seconds) {
    return advance(Duration.ofSeconds(seconds));
  }

  public MockClock advance(Duration amount) {
    now.updateAndGet(current -> current.plus(amount));
    return this;
  }

  public MockClock setInstant(Instant instant) {
    now.set(instant);
    return this;
  }

  @Override
  public ZoneId getZone() {
    return zone;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return new MockClock(now.get(), zone);
  }

  @Override
  public Instant instant() {
    return now.get();
  }
}
