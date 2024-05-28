package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.quarkus.security.UnauthorizedException;
import io.stargate.sgv2.jsonapi.JsonApiStartUp;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.inject.ConfigProperty;
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

  /** Persistence type SSTable Writer */
  public static final String OFFLINE_WRITER = "offline_writer";

  @ConfigProperty(name = "quarkus.application.name")
  String APPLICATION_NAME;

  @Inject
  public CQLSessionCache(OperationsConfig operationsConfig, MeterRegistry meterRegistry) {
    LOGGER.info("Initializing CQLSessionCache");
    this.operationsConfig = operationsConfig;
    LoadingCache<SessionCacheKey, CqlSession> loadingCache =
        Caffeine.newBuilder()
            .expireAfterAccess(
                Duration.ofSeconds(operationsConfig.databaseConfig().sessionCacheTtlSeconds()))
            .maximumSize(operationsConfig.databaseConfig().sessionCacheMaxSize())
            // removal listener is invoked after the entry has been removed from the cache. So the
            // idea is that we no longer return this session for any lookup as a first step, then
            // close the session in the background asynchronously which is a graceful closing of
            // channels i.e. any in-flight query will be completed before the session is getting
            // closed.
            .removalListener(
                (RemovalListener<SessionCacheKey, CqlSession>)
                    (sessionCacheKey, session, cause) -> {
                      if (sessionCacheKey != null) {
                        if (LOGGER.isTraceEnabled()) {
                          LOGGER.trace(
                              "Removing session for tenant : {}", sessionCacheKey.tenantId());
                        }
                      }
                      if (session != null) {
                        session.close();
                      }
                    })
            .recordStats()
            .build(this::getNewSession);
    this.sessionCache =
        CaffeineCacheMetrics.monitor(meterRegistry, loadingCache, "cql_sessions_cache");
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
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.SESSION_NAME, cacheKey.tenantId)
            .build();
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Creating new session for tenant : {}", cacheKey.tenantId());
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
      CqlSessionBuilder builder =
          new TenantAwareCqlSessionBuilder(cacheKey.tenantId())
              .withLocalDatacenter(operationsConfig.databaseConfig().localDatacenter())
              .addContactPoints(seeds)
              .withClassLoader(Thread.currentThread().getContextClassLoader())
              .withConfigLoader(loader)
              .withApplicationName(APPLICATION_NAME);
      // To use username and password, a Base64Encoded text of the credential is passed as token.
      // The text needs to be in format Cassandra:Base64(username):Base64(password)
      String token = ((TokenCredentials) cacheKey.credentials()).token();
      if (getFixedToken() == null) {
        if (token.startsWith("Cassandra:")) {
          UsernamePasswordCredentials upc = UsernamePasswordCredentials.from(token);
          builder.withAuthCredentials(
              Objects.requireNonNull(upc.userName()), Objects.requireNonNull(upc.password()));
        } else {
          throw new UnauthorizedException(
              "Invalid credentials format, expected `Cassandra:Base64(username):Base64(password)`");
        }
      } else {
        builder.withAuthCredentials(
            Objects.requireNonNull(databaseConfig.userName()),
            Objects.requireNonNull(databaseConfig.password()));
      }
      return builder.build();
    } else if (ASTRA.equals(databaseConfig.type())) {
      CqlSession cqlSession =
          new TenantAwareCqlSessionBuilder(cacheKey.tenantId())
              .withAuthCredentials(
                  TOKEN,
                  Objects.requireNonNull(((TokenCredentials) cacheKey.credentials()).token()))
              .withLocalDatacenter(operationsConfig.databaseConfig().localDatacenter())
              .withClassLoader(Thread.currentThread().getContextClassLoader())
              .withApplicationName(APPLICATION_NAME)
              .withConfigLoader(loader)
              .build();
      if (!isAstraSessionValid(cqlSession, cacheKey.tenantId())) {
        throw new UnauthorizedException(
            "Unauthorized to access tenant %s's data".formatted(cacheKey.tenantId()));
      }
      return cqlSession;
    }
    throw new RuntimeException("Unsupported database type: " + databaseConfig.type());
  }

  /**
   * This method checks if the session is valid for the tenant. If a token is generated for tenant A
   * and if it is used to access tenant B's data, the cqlsession object still gets created without
   * any error but it has no metadata or keyspaces information. So, this situation leads to return
   * misleading no keyspace found error, instead of authorization error.
   *
   * <p>This method checks if the session is valid, first by checking if there are any keyspaces and
   * returns true if there are any keyspaces. If there are no keyspaces, then it tries to execute a
   * query on system_virtual_schema.tables and returns true if the query is successful. Failure to
   * execute the query with an UnauthorizedException means the session is invalid i.e. not meant for
   * the tenant in the request.
   *
   * @param cqlSession CqlSession
   * @param tenantId tenant id
   * @return true if the session is valid, false otherwise
   */
  private boolean isAstraSessionValid(CqlSession cqlSession, String tenantId) {
    if (!cqlSession.getMetadata().getKeyspaces().isEmpty()) {
      return true;
    }
    try {
      cqlSession.execute("SELECT * FROM system_virtual_schema.tables");
      return true;
    } catch (com.datastax.oss.driver.api.core.servererrors.UnauthorizedException e) {
      LOGGER.error("Unauthorized to access tenant %s's data".formatted(tenantId), e);
      return false;
    }
  }

  /**
   * Get CQLSession from cache.
   *
   * @return CQLSession
   */
  public CqlSession getSession(DataApiRequestInfo dataApiRequestInfo) {
    String fixedToken;
    if ((fixedToken = getFixedToken()) != null
        && !dataApiRequestInfo.getCassandraToken().orElseThrow().equals(fixedToken)) {
      throw new UnauthorizedException(ErrorCode.UNAUTHENTICATED_REQUEST.getMessage());
    }
    if (!OFFLINE_WRITER.equals(operationsConfig.databaseConfig().type())) {
      return sessionCache.get(getSessionCacheKey(dataApiRequestInfo));
    } else {
      return sessionCache.getIfPresent(getSessionCacheKey(dataApiRequestInfo));
    }
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
  private SessionCacheKey getSessionCacheKey(DataApiRequestInfo dataApiRequestInfo) {
    switch (operationsConfig.databaseConfig().type()) {
      case CASSANDRA -> {
        if (dataApiRequestInfo.getCassandraToken().isPresent()) {
          return new SessionCacheKey(
              dataApiRequestInfo.getTenantId().orElse(DEFAULT_TENANT),
              new TokenCredentials(dataApiRequestInfo.getCassandraToken().orElseThrow()));
        } else {
          throw new RuntimeException(
              "Missing/Invalid authentication credentials provided for type: "
                  + operationsConfig.databaseConfig().type());
        }
      }
      case ASTRA -> {
        return new SessionCacheKey(
            dataApiRequestInfo.getTenantId().orElseThrow(),
            new TokenCredentials(dataApiRequestInfo.getCassandraToken().orElseThrow()));
      }
      case OFFLINE_WRITER -> {
        return new SessionCacheKey(dataApiRequestInfo.getTenantId().orElse(DEFAULT_TENANT), null);
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
   * Remove CQLSession from cache.
   *
   * @param cacheKey key for CQLSession cache
   */
  public void removeSession(SessionCacheKey cacheKey) {
    sessionCache.invalidate(cacheKey);
    sessionCache.cleanUp();
    LOGGER.trace("Session removed for tenant : {}", cacheKey.tenantId());
  }

  /**
   * Put CQLSession in cache.
   *
   * @param sessionCacheKey key for CQLSession cache
   * @param cqlSession CQLSession instance
   */
  public void putSession(SessionCacheKey sessionCacheKey, CqlSession cqlSession) {
    sessionCache.put(sessionCacheKey, cqlSession);
  }

  /** Key for CQLSession cache. */
  public record SessionCacheKey(String tenantId, Credentials credentials) {}

  /**
   * Credentials for CQLSession cache when username and password is provided.
   *
   * @param userName
   * @param password
   */
  private record UsernamePasswordCredentials(String userName, String password)
      implements Credentials {

    public static UsernamePasswordCredentials from(String encodedCredentials) {
      String[] parts = encodedCredentials.split(":");
      if (parts.length != 3) {
        throw new UnauthorizedException(
            "Invalid credentials format, expected `Cassandra:Base64(username):Base64(password)`");
      }
      try {
        String userName = new String(Base64.getDecoder().decode(parts[1]));
        String password = new String(Base64.getDecoder().decode(parts[2]));
        return new UsernamePasswordCredentials(userName, password);
      } catch (Exception e) {
        throw new UnauthorizedException(
            "Invalid credentials format, expected `Cassandra:Base64(username):Base64(password)`");
      }
    }
  }

  /**
   * Credentials for CQLSession cache when token is provided.
   *
   * @param token
   */
  private record TokenCredentials(String token) implements Credentials {}

  /** A marker interface for credentials. */
  private interface Credentials {}
}
