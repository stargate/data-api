package io.stargate.sgv2.jsonapi.service.cqldriver;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;

@QuarkusTest
@TestProfile(CqlSessionCacheTestOld.TestProfile.class)
// Since Quarkus 3.13, will not automatically get global test resources so need this:
@WithTestResource(DseTestResource.class)
public class CqlSessionCacheTestOld {
  public static class TestProfile implements QuarkusTestProfile {
    // Alas, we do need actual DB backend so cannot do:
    // public boolean disableGlobalTestResources() { return true; }

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("stargate.jsonapi.operations.database-config.fixed-token", "test-token");
    }
  }
  //
  //  private static final String TENANT_ID_FOR_TEST = "test_tenant";
  //
  //  @Inject OperationsConfig operationsConfig;
  //
  //  private MeterRegistry meterRegistry;
  //
  //  /**
  //   * List of sessions created in the tests. This is used to close the sessions after each test.
  // This
  //   * is needed because, though the sessions evicted from the cache are closed, the sessions left
  //   * active on the cache are not closed, so we have to close them explicitly.
  //   */
  //  private List<CqlSession> sessionsCreatedInTests;
  //
  //  @BeforeEach
  //  public void tearUpEachTest() {
  //    meterRegistry = new SimpleMeterRegistry();
  //    sessionsCreatedInTests = new ArrayList<>();
  //  }
  //
  //  @AfterEach
  //  public void tearDownEachTest() {
  //    sessionsCreatedInTests.forEach(CqlSession::close);
  //  }
  //
  //  @Test
  //  public void testOSSCxCQLSessionCacheDefaultTenant() {
  //
  //    var requestContext = mock(RequestContext.class);
  //    when(requestContext.getCassandraToken())
  //        .thenReturn(operationsConfig.databaseConfig().fixedToken());
  //
  //    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig,
  // meterRegistry);
  //    CqlSession cqlSession = cqlSessionCacheForTest.getSession(requestContext);
  //    sessionsCreatedInTests.add(cqlSession);
  //    assertThat(cqlSession.getContext().getSessionName())
  //        .as("Session name is the tenantID")
  //        .isEqualTo("default_tenant");
  //
  //    Gauge cacheSizeMetric =
  //        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
  //    assertThat(cacheSizeMetric).isNotNull();
  //    assertThat(cacheSizeMetric.value()).isEqualTo(1);
  //
  //    FunctionCounter cachePutMetric =
  //        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
  //    assertThat(cachePutMetric).isNotNull();
  //    assertThat(cachePutMetric.count()).isEqualTo(1);
  //  }
  //
  //  @Test
  //  public void testOSSCxCQLSessionCacheWithFixedToken()
  //      throws NoSuchFieldException, IllegalAccessException {
  //
  //    var requestContext = mock(RequestContext.class);
  //    when(requestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
  //    when(requestContext.getCassandraToken())
  //        .thenReturn(operationsConfig.databaseConfig().fixedToken());
  //
  //    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig,
  // meterRegistry);
  //
  //    // Monkeys with the config, for utterly unknown and unexplained reason
  //    Field operationsConfigField =
  //        cqlSessionCacheForTest.getClass().getDeclaredField("operationsConfig");
  //    operationsConfigField.setAccessible(true);
  //    operationsConfigField.set(cqlSessionCacheForTest, operationsConfig);
  //
  //    CqlSession cqlSession = cqlSessionCacheForTest.getSession(requestContext);
  //    sessionsCreatedInTests.add(cqlSession);
  //    assertThat(cqlSession.getContext().getSessionName())
  //        .as("Session name is the tenantID")
  //        .isEqualTo(TENANT_ID_FOR_TEST);
  //
  //    // metrics test
  //    Gauge cacheSizeMetric =
  //        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
  //    assertThat(cacheSizeMetric).isNotNull();
  //    assertThat(cacheSizeMetric.value()).isEqualTo(1);
  //
  //    FunctionCounter cachePutMetric =
  //        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
  //    assertThat(cachePutMetric).isNotNull();
  //    assertThat(cachePutMetric.count()).isEqualTo(1);
  //  }
  //
  //  @Test
  //  public void testOSSCxCQLSessionCacheWithInvalidFixedToken()
  //      throws NoSuchFieldException, IllegalAccessException {
  //
  //    // set request info
  //    RequestContext requestContext = mock(RequestContext.class);
  //    when(requestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
  //    when(requestContext.getCassandraToken()).thenReturn(Optional.of("invalid_token"));
  //
  //    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig,
  // meterRegistry);
  //    // set operation config
  //    Field operationsConfigField =
  //        cqlSessionCacheForTest.getClass().getDeclaredField("operationsConfig");
  //    operationsConfigField.setAccessible(true);
  //    operationsConfigField.set(cqlSessionCacheForTest, operationsConfig);
  //
  //    // Throwable
  //    Throwable t = catchThrowable(() -> cqlSessionCacheForTest.getSession(requestContext));
  //    assertThat(t)
  //        .isNotNull()
  //        .isInstanceOf(UnauthorizedException.class)
  //        .hasMessage("UNAUTHENTICATED: Invalid token");
  //
  //    // metrics test
  //    Gauge cacheSizeMetric =
  //        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
  //    assertThat(cacheSizeMetric).isNotNull();
  //    assertThat(cacheSizeMetric.value()).isEqualTo(0);
  //
  //    FunctionCounter cachePutMetric =
  //        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
  //    assertThat(cachePutMetric).isNotNull();
  //    assertThat(cachePutMetric.count()).isEqualTo(0);
  //  }
  //
  //  @Test
  //  public void testOSSCxCQLSessionCacheMultiTenant()
  //      throws NoSuchFieldException, IllegalAccessException {
  //
  //    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig,
  // meterRegistry);
  //    // set operation config
  //    Field operationsConfigField =
  //        cqlSessionCacheForTest.getClass().getDeclaredField("operationsConfig");
  //    operationsConfigField.setAccessible(true);
  //    operationsConfigField.set(cqlSessionCacheForTest, operationsConfig);
  //
  //    List<String> tenantIds = new ArrayList<>();
  //    tenantIds.add("tenant1");
  //    tenantIds.add("tenant2");
  //    tenantIds.add("tenant3");
  //    tenantIds.add("tenant4");
  //    tenantIds.add("tenant5");
  //
  //    for (String tenantId : tenantIds) {
  //      // set request info
  //      RequestContext requestContext = mock(RequestContext.class);
  //      when(requestContext.getTenantId()).thenReturn(Optional.of(tenantId));
  //      when(requestContext.getCassandraToken())
  //          .thenReturn(operationsConfig.databaseConfig().fixedToken());
  //
  //      CqlSession cqlSession = cqlSessionCacheForTest.getSession(requestContext);
  //      sessionsCreatedInTests.add(cqlSession);
  //      assertThat(cqlSession.getContext().getSessionName())
  //          .as("Session name is the tenantID")
  //          .isEqualTo(tenantId);
  //    }
  //
  //    // metrics test
  //    Gauge cacheSizeMetric =
  //        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
  //    assertThat(cacheSizeMetric).isNotNull();
  //    assertThat(cacheSizeMetric.value()).isEqualTo(tenantIds.size());
  //
  //    FunctionCounter cachePutMetric =
  //        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
  //    assertThat(cachePutMetric).isNotNull();
  //    assertThat(cachePutMetric.count()).isEqualTo(tenantIds.size());
  //
  //    FunctionCounter cacheLoadMetric =
  //        meterRegistry
  //            .find("cache.load")
  //            .tag("cache", "cql_sessions_cache")
  //            .tag("result", "success")
  //            .functionCounter();
  //    assertThat(cacheLoadMetric).isNotNull();
  //    assertThat(cacheLoadMetric.count()).isEqualTo(tenantIds.size());
  //  }
  //
  //  @Test
  //  public void testOSSCxCQLSessionCacheSizeEviction()
  //      throws NoSuchFieldException, IllegalAccessException {
  //
  //    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig,
  // meterRegistry);
  //
  //    // set operation config
  //    Field operationsConfigField =
  //        cqlSessionCacheForTest.getClass().getDeclaredField("operationsConfig");
  //    operationsConfigField.setAccessible(true);
  //    operationsConfigField.set(cqlSessionCacheForTest, operationsConfig);
  //
  //    int sessionsToCreate = operationsConfig.databaseConfig().sessionCacheMaxSize() + 5;
  //    for (int i = 0; i < sessionsToCreate; i++) {
  //      String tenantId = "tenant" + i;
  //
  //      RequestContext requestContext = mock(RequestContext.class);
  //      when(requestContext.getTenantId()).thenReturn(Optional.of(tenantId));
  //      when(requestContext.getCassandraToken())
  //          .thenReturn(operationsConfig.databaseConfig().fixedToken());
  //
  //      CqlSession cqlSession = cqlSessionCacheForTest.getSession(requestContext);
  //      sessionsCreatedInTests.add(cqlSession);
  //      assertThat(cqlSession.getContext().getSessionName())
  //          .as("Session name is the tenantID")
  //          .isEqualTo(tenantId);
  //    }
  //
  //    // test if cache size is maintained at `sessionCacheMaxSizeConfigured`
  //    long cacheSize = cqlSessionCacheForTest.cacheSize();
  //    assertThat(cacheSize).isEqualTo(operationsConfig.databaseConfig().sessionCacheMaxSize());
  //    // metrics test
  //    Gauge cacheSizeMetric =
  //        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
  //    assertThat(cacheSizeMetric).isNotNull();
  //    assertThat(cacheSizeMetric.value())
  //        .isEqualTo(operationsConfig.databaseConfig().sessionCacheMaxSize());
  //
  //    FunctionCounter cachePutMetric =
  //        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
  //    assertThat(cachePutMetric).isNotNull();
  //    assertThat(cachePutMetric.count()).isEqualTo(sessionsToCreate);
  //
  //    FunctionCounter cacheLoadMetric =
  //        meterRegistry
  //            .find("cache.load")
  //            .tag("cache", "cql_sessions_cache")
  //            .tag("result", "success")
  //            .functionCounter();
  //    assertThat(cacheLoadMetric).isNotNull();
  //    assertThat(cacheLoadMetric.count()).isEqualTo(sessionsToCreate);
  //  }
}
