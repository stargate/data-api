package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.JsonApiStartUp;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Objects;
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
  @Inject OperationsConfig operationsConfig;

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
  private final Cache<String, CqlSession> sessionCache;

  public static final String ASTRA = "astra";
  public static final String CASSANDRA = "cassandra";

  public CQLSessionCache() {
    sessionCache =
        Caffeine.newBuilder()
            .expireAfterAccess(
                Duration.ofSeconds(operationsConfig.databaseConfig().sessionCacheTtlSeconds()))
            .maximumSize(operationsConfig.databaseConfig().sessionCacheMaxSize())
            .build();
    LOGGER.info("CQLSessionCache initialized");
  }

  /**
   * Loader for new CQLSession.
   *
   * @return CQLSession
   * @throws RuntimeException if database type is not supported
   */
  private CqlSession getNewSession(String cacheKey) {
    LOGGER.info("Creating new session for : {}", cacheKey.split(":", -1)[0]);
    OperationsConfig.DatabaseConfig databaseConfig = operationsConfig.databaseConfig();
    ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
            .startProfile("slow")
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .endProfile();
    LOGGER.info("Database type: {}", databaseConfig.type());
    if (CASSANDRA.equals(databaseConfig.type())) {
      return new TenantAwareCqlSessionBuilder(
              stargateRequestInfo.getTenantId().orElse(DEFAULT_TENANT))
          .withLocalDatacenter(operationsConfig.databaseConfig().localDatacenter())
          .withConfigLoader(driverConfigLoaderBuilder.build())
          .withAuthCredentials(
              Objects.requireNonNull(databaseConfig.userName()),
              Objects.requireNonNull(databaseConfig.password()))
          .build();
    } else if (ASTRA.equals(databaseConfig.type())) {
      return new TenantAwareCqlSessionBuilder(stargateRequestInfo.getTenantId().orElseThrow())
          .withConfigLoader(driverConfigLoaderBuilder.build())
          .withAuthCredentials(
              TOKEN, Objects.requireNonNull(stargateRequestInfo.getCassandraToken().orElseThrow()))
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
   * Build key for CQLSession cache in below formats <br>
   * If backend is OSS cassandra: {tenantId}:{username}:{password} <br>
   * If backend is OSS cassandra when tenant id is not passed :
   * <b>default_tenant</b>:{username}:{password} <br>
   * If backend is AstraDB: {tenantId}:{token} <br>
   * If backend is AstraDB when tenant or token is not passed : throws exception
   *
   * @return key for CQLSession cache
   */
  private String getSessionCacheKey() {
    if (CASSANDRA.equals(operationsConfig.databaseConfig().type())) {
      return stargateRequestInfo.getTenantId().orElse(DEFAULT_TENANT)
          + ":"
          + operationsConfig.databaseConfig().userName()
          + ":"
          + operationsConfig.databaseConfig().password();
    } else if (ASTRA.equals(operationsConfig.databaseConfig().type())) {
      return stargateRequestInfo.getTenantId().orElseThrow()
          + ":"
          + stargateRequestInfo.getCassandraToken().orElseThrow();
    }
    throw new RuntimeException(
        "Unsupported database type: " + operationsConfig.databaseConfig().type());
  }
}
