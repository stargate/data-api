package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.quarkus.security.UnauthorizedException;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.JsonApiStartUp;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CQL session cache to reuse the session for the same tenant and token. The cache is configured to
 * expire after <code>CACHE_TTL_SECONDS</code> of inactivity and to have a maximum size of <code>
 * CACHE_TTL_SECONDS</code> sessions.
 */
@ApplicationScoped
public class CQLSessionCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonApiStartUp.class);

  /** Configuration for the JSON API operations. */
  private final OperationsConfig operationsConfig;

  /** Stargate request info. */
  @Inject StargateRequestInfo stargateRequestInfo;

  /**
   * Default tenant to be used when the backend is OSS cassandra and when no tenant is passed in the
   * request
   */
  private static final String DEFAULT_TENANT = "default_tenant";
  /** CQL username to be used when the backend is AstraDB */
  private static final String TOKEN = "token";
  /** CQLSession cache. */
  private final LoadingCache<SessionCacheKey, CqlSession> sessionCache;
  /** Database type Astra */
  public static final String ASTRA = "astra";
  /** Database type OSS cassandra */
  public static final String CASSANDRA = "cassandra";

  @Inject
  public CQLSessionCache(OperationsConfig operationsConfig, MeterRegistry meterRegistry) {
    this.operationsConfig = operationsConfig;
    LoadingCache<SessionCacheKey, CqlSession> sessionCache =
        Caffeine.newBuilder()
            .expireAfterAccess(
                Duration.ofSeconds(operationsConfig.databaseConfig().sessionCacheTtlSeconds()))
            .maximumSize(operationsConfig.databaseConfig().sessionCacheMaxSize())
            .evictionListener(
                (RemovalListener<SessionCacheKey, CqlSession>)
                    (sessionCacheKey, session, cause) -> {
                      if (sessionCacheKey != null) {
                        if (LOGGER.isTraceEnabled()) {
                          LOGGER.trace(
                              "Removing session for tenant : {}", sessionCacheKey.tenantId);
                        }
                      }
                      if (session != null) {
                        session.close();
                      }
                    })
            .recordStats()
            .build(this::getNewSession);
    this.sessionCache =
        CaffeineCacheMetrics.monitor(meterRegistry, sessionCache, "cql_sessions_cache");
    LOGGER.info(
        "CQLSessionCache initialized with ttl of {} seconds and max size of {}",
        operationsConfig.databaseConfig().sessionCacheTtlSeconds(),
        operationsConfig.databaseConfig().sessionCacheMaxSize());
  }

  /**
   * Loader for new CQLSession.
   *
   * @return CQLSession
   * @throws RuntimeException if database type is not supported
   */
  private CqlSession getNewSession(SessionCacheKey cacheKey) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Creating new session for tenant : {}", cacheKey.tenantId);
    }
    OperationsConfig.DatabaseConfig databaseConfig = operationsConfig.databaseConfig();
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Database type: {}", databaseConfig.type());
    }
    if (CASSANDRA.equals(databaseConfig.type())) {
      List<InetSocketAddress> seeds =
          Objects.requireNonNull(operationsConfig.databaseConfig().cassandraEndPoints()).stream()
              .map(
                  host ->
                      new InetSocketAddress(
                          host, operationsConfig.databaseConfig().cassandraPort()))
              .collect(Collectors.toList());

      return new TenantAwareCqlSessionBuilder(
              stargateRequestInfo.getTenantId().orElse(DEFAULT_TENANT))
          .withLocalDatacenter(operationsConfig.databaseConfig().localDatacenter())
          .addContactPoints(seeds)
          .withAuthCredentials(
              Objects.requireNonNull(databaseConfig.userName()),
              Objects.requireNonNull(databaseConfig.password()))
          .build();
    } else if (ASTRA.equals(databaseConfig.type())) {
      return new TenantAwareCqlSessionBuilder(stargateRequestInfo.getTenantId().orElseThrow())
          .withAuthCredentials(
              TOKEN, Objects.requireNonNull(stargateRequestInfo.getCassandraToken().orElseThrow()))
          .withLocalDatacenter(operationsConfig.databaseConfig().localDatacenter())
          .build();
    }
    throw new RuntimeException("Unsupported database type: " + databaseConfig.type());
  }

  /**
   * Get CQLSession from cache.
   *
   * @return CQLSession
   */
  public CqlSession getSession() {
    String fixedToken;
    if ((fixedToken = getFixedToken()) != null
        && !stargateRequestInfo.getCassandraToken().orElseThrow().equals(fixedToken)) {
      throw new UnauthorizedException("Unauthorized");
    }
    return sessionCache.get(getSessionCacheKey());
  }

  /**
   * Default token which will be used by the integration tests. If this property is set, then the
   * token from the request will be compared with this to perform authentication.
   */
  private String getFixedToken() {
    return operationsConfig.databaseConfig().fixedToken().orElse(null);
  }

  /**
   * Build key for CQLSession cache from tenant and token if the database type is AstraDB or from
   * tenant, username and password if the database type is OSS cassandra (also, if token is present
   * in the request, that will be given priority for the cache key).
   *
   * @return key for CQLSession cache
   */
  private SessionCacheKey getSessionCacheKey() {
    switch (operationsConfig.databaseConfig().type()) {
      case CASSANDRA -> {
        if (stargateRequestInfo.getCassandraToken().isPresent()) {
          return new SessionCacheKey(
              stargateRequestInfo.getTenantId().orElse(DEFAULT_TENANT),
              new TokenCredentials(stargateRequestInfo.getCassandraToken().orElseThrow()));
        }
        return new SessionCacheKey(
            stargateRequestInfo.getTenantId().orElse(DEFAULT_TENANT),
            new UsernamePasswordCredentials(
                operationsConfig.databaseConfig().userName(),
                operationsConfig.databaseConfig().password()));
      }
      case ASTRA -> {
        return new SessionCacheKey(
            stargateRequestInfo.getTenantId().orElseThrow(),
            new TokenCredentials(stargateRequestInfo.getCassandraToken().orElseThrow()));
      }
    }
    throw new RuntimeException(
        "Unsupported database type: " + operationsConfig.databaseConfig().type());
  }

  /**
   * Get cache size.
   *
   * @return cache size
   */
  public long cacheSize() {
    sessionCache.cleanUp();
    return sessionCache.estimatedSize();
  }

  /**
   * Key for CQLSession cache.
   *
   * @param tenantId tenant id
   * @param credentials credentials (username/password or token)
   */
  private record SessionCacheKey(String tenantId, Credentials credentials) {}

  /**
   * Credentials for CQLSession cache when username and password is provided.
   *
   * @param userName
   * @param password
   */
  private record UsernamePasswordCredentials(String userName, String password)
      implements Credentials {}

  /**
   * Credentials for CQLSession cache when token is provided.
   *
   * @param token
   */
  private record TokenCredentials(String token) implements Credentials {}

  /** A marker interface for credentials. */
  private interface Credentials {}
}
