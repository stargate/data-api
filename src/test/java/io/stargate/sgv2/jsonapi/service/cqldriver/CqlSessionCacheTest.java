package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.service.cqldriver.TenantAwareCqlSessionBuilderTest.TENANT_ID_PROPERTY_KEY;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class CqlSessionCacheTest {

  @InjectMock protected StargateRequestInfo stargateRequestInfo;

  @Inject CQLSessionCache cqlSessionCache;

  @Inject OperationsConfig operationsConfig;

  @Test
  public void testOSSCxCQLSessionCache() {
    CqlSession cqlSession = cqlSessionCache.getSession();
    assertThat(
            ((DefaultDriverContext) cqlSession.getContext())
                .getStartupOptions()
                .get(TENANT_ID_PROPERTY_KEY))
        .isEqualTo("default_tenant");
  }
}
