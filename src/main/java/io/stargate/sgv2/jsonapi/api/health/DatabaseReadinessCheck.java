package io.stargate.sgv2.jsonapi.api.health;

import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.google.common.annotations.VisibleForTesting;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Database probe behind {@code GET /v1/health/ready}, constructed by the JAX-RS resource - not a
 * CDI bean or a MicroProfile health check.
 *
 * <p>Gets a session from the {@link CQLSessionCache} and succeeds when the session metadata has at
 * least one {@link NodeState#UP} node. No query is issued: session creation fails if the database
 * is unreachable, and the driver keeps node states current for cached sessions.
 */
public final class DatabaseReadinessCheck {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

  private final Supplier<CQLSessionCache> sessionCacheSupplier;
  private final Duration timeout;

  public DatabaseReadinessCheck(Supplier<CQLSessionCache> sessionCacheSupplier) {
    this(sessionCacheSupplier, DEFAULT_TIMEOUT);
  }

  @VisibleForTesting
  DatabaseReadinessCheck(CQLSessionCache sessionCache, Duration timeout) {
    this(() -> sessionCache, timeout);
  }

  private DatabaseReadinessCheck(Supplier<CQLSessionCache> sessionCacheSupplier, Duration timeout) {
    this.sessionCacheSupplier =
        Objects.requireNonNull(sessionCacheSupplier, "sessionCacheSupplier must not be null");
    this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
  }

  /** Fails unless the session metadata has at least one {@link NodeState#UP} node. */
  public Uni<Void> check(RequestContext requestContext) {
    Objects.requireNonNull(requestContext, "requestContext must not be null");

    return Uni.createFrom()
        .deferred(
            () -> {
              var sessionCache =
                  Objects.requireNonNull(
                      sessionCacheSupplier.get(), "sessionCacheSupplier returned null");
              return sessionCache
                  .getSession(requestContext)
                  .invoke(
                      session -> {
                        var anyNodeUp =
                            session.getMetadata().getNodes().values().stream()
                                .anyMatch(node -> node.getState() == NodeState.UP);
                        if (!anyNodeUp) {
                          throw new IllegalStateException(
                              "Session metadata has no node in the UP state");
                        }
                      })
                  .replaceWithVoid();
            })
        // bounds session acquisition, the metadata inspection is in-memory
        .ifNoItem()
        .after(timeout)
        .fail();
  }
}
