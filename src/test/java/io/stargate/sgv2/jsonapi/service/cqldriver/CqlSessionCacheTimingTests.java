package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.service.cqldriver.TenantAwareCqlSessionBuilderTest.TENANT_ID_PROPERTY_KEY;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(FixedTokenTimingTestProfile.class)
@Disabled(
    "Disabled time based eviction test for now, since adding this test causes the CqlSessionCacheTests tests to fail")
public class CqlSessionCacheTimingTests {

  @Inject OperationsConfig operationsConfig;

  private MeterRegistry meterRegistry;

  @BeforeEach
  public void setupEachTest() {
    meterRegistry = new SimpleMeterRegistry();
  }

  @Test
  public void testOSSCxCQLSessionCacheTimedEviction()
      throws NoSuchFieldException, IllegalAccessException, InterruptedException {
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig, meterRegistry);
    int sessionsToCreate = operationsConfig.databaseConfig().sessionCacheMaxSize();
    for (int i = 0; i < sessionsToCreate; i++) {
      String tenantId = "tenant_timing_test_" + i;
      StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
      when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of(tenantId));
      when(stargateRequestInfo.getCassandraToken())
          .thenReturn(operationsConfig.databaseConfig().fixedToken());
      Field stargateRequestInfoField =
          cqlSessionCacheForTest.getClass().getDeclaredField("stargateRequestInfo");
      stargateRequestInfoField.setAccessible(true);
      stargateRequestInfoField.set(cqlSessionCacheForTest, stargateRequestInfo);

      assertThat(
              ((DefaultDriverContext) cqlSessionCacheForTest.getSession().getContext())
                  .getStartupOptions()
                  .get(TENANT_ID_PROPERTY_KEY))
          .isEqualTo(tenantId);
      CqlSession cqlSession = cqlSessionCacheForTest.getSession();
      assertThat(cqlSession).isNotNull();
    }
    Thread.sleep(10000);
    assertThat(cqlSessionCacheForTest.cacheSize()).isEqualTo(0);
    // metrics test
    Gauge cacheSizeMetric =
        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
    assertThat(cacheSizeMetric).isNotNull();
    assertThat(cacheSizeMetric.value()).isEqualTo(0);
    FunctionCounter cachePutMetric =
        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
    assertThat(cachePutMetric).isNotNull();
    assertThat(cachePutMetric.count()).isEqualTo(sessionsToCreate);
    FunctionCounter cacheLoadMetric =
        meterRegistry
            .find("cache.load")
            .tag("cache", "cql_sessions_cache")
            .tag("result", "success")
            .functionCounter();
    assertThat(cacheLoadMetric).isNotNull();
    assertThat(cacheLoadMetric.count()).isEqualTo(sessionsToCreate);
  }
}
