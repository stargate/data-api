package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.service.cqldriver.TenantAwareCqlSessionBuilderTest.TENANT_ID_PROPERTY_KEY;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.util.Optional;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class CqlSessionCacheTest {

  private static final String TENANT_ID_FOR_TEST = "test_tenant";

  @InjectMock protected StargateRequestInfo stargateRequestInfo;

  @Inject CQLSessionCache cqlSessionCache;

  @Inject OperationsConfig operationsConfig;

  // TODO @Kathir, what is the difference of this class and FixedTokenTests
  @Test
  public void testOSSCxCQLSessionCache() throws NoSuchFieldException, IllegalAccessException {

    //    @Kathir
    //    CqlSession cqlSession = cqlSessionCache.getSession();
    //    assertThat(
    //            ((DefaultDriverContext) cqlSession.getContext())
    //                .getStartupOptions()
    //                .get(TENANT_ID_PROPERTY_KEY))
    //        .isEqualTo("default_tenant");

    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
    when(stargateRequestInfo.getCassandraToken())
        .thenReturn(Optional.ofNullable(operationsConfig.databaseConfig().fixedToken()));
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig);
    Field stargateRequestInfoField =
        cqlSessionCacheForTest.getClass().getDeclaredField("stargateRequestInfo");
    stargateRequestInfoField.setAccessible(true);
    stargateRequestInfoField.set(cqlSessionCacheForTest, stargateRequestInfo);
    assertThat(
            ((DefaultDriverContext) cqlSessionCacheForTest.getSession().getContext())
                .getStartupOptions()
                .get(TENANT_ID_PROPERTY_KEY))
        .isEqualTo(TENANT_ID_FOR_TEST);
  }
}
