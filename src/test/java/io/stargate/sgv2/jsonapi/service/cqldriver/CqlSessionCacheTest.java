package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.service.cqldriver.TenantAwareCqlSessionBuilderTest.TENANT_ID_PROPERTY_KEY;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.util.Optional;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(FixedTokenOverrideProfile.class)
public class CqlSessionCacheTest {

  private static final String TENANT_ID_FOR_TEST = "test_tenant";

  @Inject OperationsConfig operationsConfig;

  @Inject MeterRegistry meterRegistry;

  @Test
  public void testOSSCxCQLSessionCacheDefaultTenant()
      throws NoSuchFieldException, IllegalAccessException {
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    when(stargateRequestInfo.getCassandraToken())
        .thenReturn(operationsConfig.databaseConfig().fixedToken());
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig, meterRegistry);
    Field stargateRequestInfoField =
        cqlSessionCacheForTest.getClass().getDeclaredField("stargateRequestInfo");
    stargateRequestInfoField.setAccessible(true);
    stargateRequestInfoField.set(cqlSessionCacheForTest, stargateRequestInfo);
    // set operation config
    Field operationsConfigField =
        cqlSessionCacheForTest.getClass().getDeclaredField("operationsConfig");
    operationsConfigField.setAccessible(true);
    operationsConfigField.set(cqlSessionCacheForTest, operationsConfig);
    // set meter registry
    Field meterRegistryField = cqlSessionCacheForTest.getClass().getDeclaredField("meterRegistry");
    meterRegistryField.setAccessible(true);
    meterRegistryField.set(cqlSessionCacheForTest, new SimpleMeterRegistry());
    assertThat(
            ((DefaultDriverContext) cqlSessionCacheForTest.getSession().getContext())
                .getStartupOptions()
                .get(TENANT_ID_PROPERTY_KEY))
        .isEqualTo("default_tenant");
  }

  @Test
  public void testOSSCxCQLSessionCache() throws NoSuchFieldException, IllegalAccessException {
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
    when(stargateRequestInfo.getCassandraToken())
        .thenReturn(operationsConfig.databaseConfig().fixedToken());
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig, meterRegistry);
    Field stargateRequestInfoField =
        cqlSessionCacheForTest.getClass().getDeclaredField("stargateRequestInfo");
    stargateRequestInfoField.setAccessible(true);
    stargateRequestInfoField.set(cqlSessionCacheForTest, stargateRequestInfo);
    // set operation config
    Field operationsConfigField =
        cqlSessionCacheForTest.getClass().getDeclaredField("operationsConfig");
    operationsConfigField.setAccessible(true);
    operationsConfigField.set(cqlSessionCacheForTest, operationsConfig);
    // set meter registry
    Field meterRegistryField = cqlSessionCacheForTest.getClass().getDeclaredField("meterRegistry");
    meterRegistryField.setAccessible(true);
    meterRegistryField.set(cqlSessionCacheForTest, new SimpleMeterRegistry());
    assertThat(
            ((DefaultDriverContext) cqlSessionCacheForTest.getSession().getContext())
                .getStartupOptions()
                .get(TENANT_ID_PROPERTY_KEY))
        .isEqualTo(TENANT_ID_FOR_TEST);
  }
}
