package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.service.cqldriver.TenantAwareCqlSessionBuilderTest.TENANT_ID_PROPERTY_KEY;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.cache.CaffeineCache;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(FixedTokenTimingTestProfile.class)
public class CqlSessionCacheTimingTests {
  private static final String TENANT_ID_FOR_TEST = "test_tenant";

  @Inject
  @CacheName("cql-sessions-cache")
  Cache sessionCache;

  @Inject OperationsConfig operationsConfig;

  @Test
  public void testOSSCxCQLSessionCache()
      throws NoSuchFieldException, IllegalAccessException, ExecutionException,
          InterruptedException {
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
    Optional<String> tokenForTest = operationsConfig.databaseConfig().fixedToken();
    assertThat(tokenForTest).isPresent();
    when(stargateRequestInfo.getCassandraToken()).thenReturn(tokenForTest);
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig);
    Field stargateRequestInfoField =
        cqlSessionCacheForTest.getClass().getDeclaredField("stargateRequestInfo");
    stargateRequestInfoField.setAccessible(true);
    stargateRequestInfoField.set(cqlSessionCacheForTest, stargateRequestInfo);
    // set cache
    Field sessionCacheField = cqlSessionCacheForTest.getClass().getDeclaredField("sessionCache");
    sessionCacheField.setAccessible(true);
    sessionCacheField.set(cqlSessionCacheForTest, sessionCache);
    CqlSession cqlSession =
        cqlSessionCacheForTest
            .getSession()
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();
    assertThat(
            ((DefaultDriverContext) cqlSession.getContext())
                .getStartupOptions()
                .get(TENANT_ID_PROPERTY_KEY))
        .isEqualTo(TENANT_ID_FOR_TEST);
    // test from underlying caffeine cache
    CQLSessionCache.SessionCacheKey sessionCacheKey =
        new CQLSessionCache.SessionCacheKey(
            TENANT_ID_FOR_TEST, new CQLSessionCache.TokenCredentials(tokenForTest.get()));
    CaffeineCache caffeineCache = sessionCache.as(CaffeineCache.class);
    assertThat(caffeineCache.keySet().size()).isEqualTo(1);
    CqlSession cqlSessionFromCache = (CqlSession) caffeineCache.getIfPresent(sessionCacheKey).get();
    assertThat(cqlSessionFromCache).isNotNull();
    assertThat(
            ((DefaultDriverContext) cqlSessionFromCache.getContext())
                .getStartupOptions()
                .get(TENANT_ID_PROPERTY_KEY))
        .isEqualTo(TENANT_ID_FOR_TEST);
    Thread.sleep(2000);
    assertThat(caffeineCache.keySet().size()).isEqualTo(0);
  }
}
