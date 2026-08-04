package io.stargate.sgv2.jsonapi.api.health;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.google.common.annotations.VisibleForTesting;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Runs the database probe exposed at {@code GET /v1/health/ready}.
 *
 * <p>This class is constructed by the JAX-RS resource and is not a CDI bean or a MicroProfile
 * health check. The caller's request context supplies the tenant, token, and User-Agent for both
 * Astra and Cassandra connections.
 */
public final class DatabaseReadinessCheck {

  private static final String READINESS_QUERY = "SELECT * FROM datastax_sla.check LIMIT 1";
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

  private final Supplier<CQLSessionCache> sessionCacheSupplier;
  private final SimpleStatement statement;
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
    this.statement =
        SimpleStatement.builder(READINESS_QUERY)
            .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
            .setTimeout(timeout)
            .build();
  }

  /**
   * Executes a replicated table read at {@code LOCAL_QUORUM}, using the {@code table-read} driver
   * profile for the remaining read settings.
   */
  public Uni<Void> check(RequestContext requestContext) {
    Objects.requireNonNull(requestContext, "requestContext must not be null");

    return Uni.createFrom()
        .deferred(
            () -> {
              var sessionCache =
                  Objects.requireNonNull(
                      sessionCacheSupplier.get(), "sessionCacheSupplier returned null");
              return new CommandQueryExecutor(
                      sessionCache, requestContext, CommandQueryExecutor.QueryTarget.TABLE)
                  .executeRead(statement)
                  .replaceWithVoid();
            })
        // The statement timeout bounds driver I/O; this also bounds asynchronous session lookup.
        .ifNoItem()
        .after(timeout)
        .fail();
  }
}
