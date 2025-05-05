package io.stargate.sgv2.jsonapi.service.cqldriver;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;

@QuarkusTest
@TestProfile(InvalidCqlCredentialsTest.TestProfile.class)
// Since Quarkus 3.13, will not automatically get global test resources so need this:
@WithTestResource(DseTestResource.class)
public class InvalidCqlCredentialsTest {
  public static class TestProfile implements QuarkusTestProfile {
    // Alas, we do need actual DB backend so cannot do:
    // public boolean disableGlobalTestResources() { return true; }

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "stargate.jsonapi.operations.database-config.fixed-token",
          "test-token",
          "stargate.jsonapi.operations.database-config.password",
          "invalid-password");
    }
  }

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
  //  public void testOSSCxCQLSessionCacheWithInvalidCredentials()
  //      throws NoSuchFieldException, IllegalAccessException {
  //    // set request info
  //    RequestContext dataApiRequestInfo = mock(RequestContext.class);
  //    when(dataApiRequestInfo.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
  //    when(dataApiRequestInfo.getCassandraToken())
  //        .thenReturn(operationsConfig.databaseConfig().fixedToken());
  //    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig,
  // meterRegistry);
  //    // Throwable
  //    Throwable t = catchThrowable(() -> cqlSessionCacheForTest.getSession(dataApiRequestInfo));
  //    assertThat(t).isInstanceOf(AllNodesFailedException.class);
  //    CommandResult.Error error =
  //        ThrowableToErrorMapper.getMapperWithMessageFunction().apply(t, t.getMessage());
  //    assertThat(error).isNotNull();
  //    assertThat(error.message()).contains("UNAUTHENTICATED: Invalid token");
  //    assertThat(error.httpStatus()).isEqualTo(Response.Status.UNAUTHORIZED);
  //    assertThat(cqlSessionCacheForTest.cacheSize()).isEqualTo(0);
  //    // metrics test
  //    Gauge cacheSizeMetric =
  //        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
  //    assertThat(cacheSizeMetric).isNotNull();
  //    assertThat(cacheSizeMetric.value()).isEqualTo(0);
  //    FunctionCounter cachePutMetric =
  //        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
  //    assertThat(cachePutMetric).isNotNull();
  //    assertThat(cachePutMetric.count()).isEqualTo(1);
  //    FunctionCounter cacheLoadSuccessMetric =
  //        meterRegistry
  //            .find("cache.load")
  //            .tag("cache", "cql_sessions_cache")
  //            .tag("result", "success")
  //            .functionCounter();
  //    assertThat(cacheLoadSuccessMetric).isNotNull();
  //    assertThat(cacheLoadSuccessMetric.count()).isEqualTo(0);
  //    FunctionCounter cacheLoadFailureMetric =
  //        meterRegistry
  //            .find("cache.load")
  //            .tag("cache", "cql_sessions_cache")
  //            .tag("result", "failure")
  //            .functionCounter();
  //    assertThat(cacheLoadFailureMetric).isNotNull();
  //    assertThat(cacheLoadFailureMetric.count()).isEqualTo(1);
  //  }
}
