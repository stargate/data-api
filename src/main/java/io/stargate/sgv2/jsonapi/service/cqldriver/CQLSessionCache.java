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
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

@ApplicationScoped
public class CQLSessionCache {
  @Inject OperationsConfig operationsConfig;

  @Inject StargateRequestInfo stargateRequestInfo;

  private static final long CACHE_TTL_SECONDS = 300;
  private static final long CACHE_MAX_SIZE = 1000;

  private final Cache<String, CqlSession> sessionCache =
      Caffeine.newBuilder()
          .expireAfterAccess(Duration.ofSeconds(CACHE_TTL_SECONDS))
          .maximumSize(CACHE_MAX_SIZE)
          .build(cacheKey -> getNewSession());

  private CqlSession getNewSession() {
    OperationsConfig.DatabaseConfig databaseConfig = operationsConfig.databaseConfig();
    ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder =
        DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
            .startProfile("slow")
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .endProfile();
    if ("cassandra".equals(databaseConfig.type())) {
      return CqlSession.builder()
          .withConfigLoader(driverConfigLoaderBuilder.build())
          .withAuthCredentials(
              Objects.requireNonNull(databaseConfig.userName()),
              Objects.requireNonNull(databaseConfig.password()))
          .build();
    } else if ("astra".equals(databaseConfig.type())) {
      return CqlSession.builder()
          .withConfigLoader(driverConfigLoaderBuilder.build())
          .withAuthCredentials("token", Objects.requireNonNull(databaseConfig.token()))
          // TODO CQL - remove secure connect bundle after integrating with non TLS router
          .withCloudSecureConnectBundle(
              Path.of(Objects.requireNonNull(databaseConfig.secureConnectBundlePath())))
          .build();
    }
    throw new RuntimeException("Unsupported database type: " + databaseConfig.type());
  }

  public CqlSession getSession() {
    return sessionCache.getIfPresent(getSessionCacheKey());
  }

  private String getSessionCacheKey() {
    return stargateRequestInfo.getTenantId() + ":" + stargateRequestInfo.getCassandraToken();
  }
}
