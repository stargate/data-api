package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.service.cqldriver.TenantAwareCqlSessionBuilderTest.TENANT_ID_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.cache.CaffeineCache;
import io.quarkus.security.UnauthorizedException;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(FixedTokenOverrideProfile.class)
public class CqlSessionCacheTests {

  private static final String TENANT_ID_FOR_TEST = "test_tenant";

  @Inject
  @CacheName("cql-sessions-cache")
  Cache sessionCache;

  @Inject OperationsConfig operationsConfig;

  @ConfigProperty(name = "quarkus.cache.caffeine.cql-sessions-cache.maximum-size")
  int sessionCacheMaxSizeConfigured;

  @BeforeEach
  public void setup() {
    sessionCache
        .invalidateAll()
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem();
  }

  @Test
  public void testOSSCxCQLSessionCacheDefaultTenant()
      throws NoSuchFieldException, IllegalAccessException {
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    when(stargateRequestInfo.getCassandraToken())
        .thenReturn(operationsConfig.databaseConfig().fixedToken());
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
        .isEqualTo("default_tenant");
  }

  @Test
  public void testOSSCxCQLSessionCacheWithFixedToken()
      throws NoSuchFieldException, IllegalAccessException {
    // set request info
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
    when(stargateRequestInfo.getCassandraToken())
        .thenReturn(operationsConfig.databaseConfig().fixedToken());
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
  }

  @Test
  public void testOSSCxCQLSessionCacheWithInvalidFixedToken()
      throws NoSuchFieldException, IllegalAccessException {
    // set request info
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
    when(stargateRequestInfo.getCassandraToken()).thenReturn(Optional.of("invalid_token"));
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig);
    Field stargateRequestInfoField =
        cqlSessionCacheForTest.getClass().getDeclaredField("stargateRequestInfo");
    stargateRequestInfoField.setAccessible(true);
    stargateRequestInfoField.set(cqlSessionCacheForTest, stargateRequestInfo);
    // set cache
    Field sessionCacheField = cqlSessionCacheForTest.getClass().getDeclaredField("sessionCache");
    sessionCacheField.setAccessible(true);
    sessionCacheField.set(cqlSessionCacheForTest, sessionCache);
    // Throwable
    Throwable t = catchThrowable(cqlSessionCacheForTest::getSession);
    assertThat(t).isNotNull().isInstanceOf(UnauthorizedException.class).hasMessage("Unauthorized");
  }

  @Test
  public void testOSSCxCQLSessionCacheMultiTenant()
      throws NoSuchFieldException, IllegalAccessException {
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig);
    // set cache
    Field sessionCacheField = cqlSessionCacheForTest.getClass().getDeclaredField("sessionCache");
    sessionCacheField.setAccessible(true);
    sessionCacheField.set(cqlSessionCacheForTest, sessionCache);

    List<String> tenantIds = new ArrayList<>();
    tenantIds.add("tenant1");
    tenantIds.add("tenant2");
    for (String tenantId : tenantIds) {
      StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
      when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of(tenantId));
      when(stargateRequestInfo.getCassandraToken())
          .thenReturn(operationsConfig.databaseConfig().fixedToken());

      Field stargateRequestInfoField =
          cqlSessionCacheForTest.getClass().getDeclaredField("stargateRequestInfo");
      stargateRequestInfoField.setAccessible(true);
      stargateRequestInfoField.set(cqlSessionCacheForTest, stargateRequestInfo);

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
          .isEqualTo(tenantId);
    }
  }

  @Test
  public void testOSSCxCQLSessionCacheEviction()
      throws NoSuchFieldException, IllegalAccessException {
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig);
    // set cache
    Field sessionCacheField = cqlSessionCacheForTest.getClass().getDeclaredField("sessionCache");
    sessionCacheField.setAccessible(true);
    sessionCacheField.set(cqlSessionCacheForTest, sessionCache);
    // create some extra sessions than the max size
    int sessionsToCreate = sessionCacheMaxSizeConfigured + 5;
    for (int i = 0; i < sessionsToCreate; i++) {
      String tenantId = "tenant" + i;
      StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
      when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of(tenantId));
      when(stargateRequestInfo.getCassandraToken())
          .thenReturn(operationsConfig.databaseConfig().fixedToken());

      Field stargateRequestInfoField =
          cqlSessionCacheForTest.getClass().getDeclaredField("stargateRequestInfo");
      stargateRequestInfoField.setAccessible(true);
      stargateRequestInfoField.set(cqlSessionCacheForTest, stargateRequestInfo);

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
          .isEqualTo(tenantId);
    }
    // test if cache size is maintained at `sessionCacheMaxSizeConfigured`
    CaffeineCache caffeineCache = sessionCache.as(CaffeineCache.class);
    assertThat(caffeineCache.keySet().size()).isEqualTo(sessionCacheMaxSizeConfigured);
  }
}
