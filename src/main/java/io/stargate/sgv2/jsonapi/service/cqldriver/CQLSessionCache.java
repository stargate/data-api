package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Objects;

/**
 * CQL session cache to reuse the session for the same tenant and token. The cache is configured to
 * expire after <code>CACHE_TTL_SECONDS</code> of inactivity and to have a maximum size of <code>
 * CACHE_TTL_SECONDS</code> sessions.
 */
@ApplicationScoped
public class CQLSessionCache {
  /** Configuration for the JSON API operations. */
  @Inject OperationsConfig operationsConfig;

  /** Stargate request info. */
  @Inject StargateRequestInfo stargateRequestInfo;

  /** Time to live for CQLSession in cache in seconds. */
  private static final long CACHE_TTL_SECONDS = 60;
  /** Maximum number of CQLSessions in cache. */
  private static final long CACHE_MAX_SIZE = 100;

  /** CQLSession cache. */
  private final Cache<String, CqlSession> sessionCache =
      Caffeine.newBuilder()
          .expireAfterAccess(Duration.ofSeconds(CACHE_TTL_SECONDS))
          .maximumSize(CACHE_MAX_SIZE)
          .build();

  public static final String ASTRA = "astra";
  public static final String CASSANDRA = "cassandra";

  /**
   * Loader for new CQLSession.
   *
   * @return CQLSession
   * @throws RuntimeException if database type is not supported
   */
  private CqlSession getNewSession(String cacheKey) {
    OperationsConfig.DatabaseConfig databaseConfig = operationsConfig.databaseConfig();
    ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder =
        DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
            .startProfile("slow")
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .endProfile();
    if (CASSANDRA.equals(databaseConfig.type())) {
      return new TenantAwareCqlSessionBuilder(stargateRequestInfo.getTenantId().orElse(null))
          .withLocalDatacenter(operationsConfig.databaseConfig().localDatacenter())
          .withConfigLoader(driverConfigLoaderBuilder.build())
          .withAuthCredentials(
              Objects.requireNonNull(databaseConfig.userName()),
              Objects.requireNonNull(databaseConfig.password()))
          .build();
    } else if (ASTRA.equals(databaseConfig.type())) {
      return new TenantAwareCqlSessionBuilder(stargateRequestInfo.getTenantId().orElse(null))
          .withConfigLoader(driverConfigLoaderBuilder.build())
          .withAuthCredentials("token", Objects.requireNonNull(databaseConfig.token()))
          .withLocalDatacenter(operationsConfig.databaseConfig().localDatacenter())
          /*.withCloudSecureConnectBundle(
          Path.of(Objects.requireNonNull(databaseConfig.secureConnectBundlePath())))*/
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
    return sessionCache.get(getSessionCacheKey(), this::getNewSession);
  }

  /**
   * Build key for CQLSession cache.
   *
   * @return key
   */
  private String getSessionCacheKey() {
    if (CASSANDRA.equals(operationsConfig.databaseConfig().type())) {
      return stargateRequestInfo.getTenantId()
          + ":"
          + operationsConfig.databaseConfig().userName()
          + ":"
          + operationsConfig.databaseConfig().password();
    } else if (ASTRA.equals(operationsConfig.databaseConfig().type())) {
      return stargateRequestInfo.getTenantId() + ":" + stargateRequestInfo.getCassandraToken();
    }
    throw new RuntimeException(
        "Unsupported database type: " + operationsConfig.databaseConfig().type());
  }
}
