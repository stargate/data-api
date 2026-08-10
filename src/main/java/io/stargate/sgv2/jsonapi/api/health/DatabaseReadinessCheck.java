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
 * Runs the database probe exposed at {@code GET /v1/health/ready}.
 *
 * <p>The probe obtains a session through the normal session cache and inspects the driver session
 * metadata, which the driver populates from {@code system.local} and {@code system.peers} and keeps
 * current through node state events. The pod is ready when at least one node is {@link
 * NodeState#UP}. Acquiring the session is itself part of the check: a pod that cannot connect to
 * the database fails session creation and reports {@code DOWN}, and no query is issued against the
 * database.
 *
 * <p>This class is constructed by the JAX-RS resource and is not a CDI bean or a MicroProfile
 * health check. The caller's request context supplies the tenant, token, and User-Agent for both
 * Astra and Cassandra connections.
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

  /**
   * Obtains a session from the cache and fails unless the session metadata has at least one node in
   * the {@link NodeState#UP} state.
   */
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
        // Bounds asynchronous session acquisition, the metadata inspection is in-memory only.
        .ifNoItem()
        .after(timeout)
        .fail();
  }
}
