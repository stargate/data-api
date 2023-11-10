package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.service.cqldriver.TenantAwareCqlSessionBuilderTest.TENANT_ID_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.quarkus.security.UnauthorizedException;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;
import org.junit.jupiter.api.Disabled;
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

  @Test
  public void testOSSCxCQLSessionCacheWithFixedToken() {
    try {
      final String fixedTenant = "fixed_tenant";
      System.setProperty(CQLSessionCache.FIXED_TOKEN_PROPERTY_NAME, fixedTenant);
      when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of(fixedTenant));
      CqlSession cqlSession = cqlSessionCache.getSession();
      assertThat(
              ((DefaultDriverContext) cqlSession.getContext())
                  .getStartupOptions()
                  .get(TENANT_ID_PROPERTY_KEY))
          .isEqualTo(fixedTenant);
    } finally {
      System.clearProperty(CQLSessionCache.FIXED_TOKEN_PROPERTY_NAME);
    }
  }

  @Disabled("TODO: fix this test")
  @Test
  public void testOSSCxCQLSessionCacheWithInvalidFixedToken()
      throws NoSuchFieldException, IllegalAccessException {
    final String fixedTenant = "fixed_tenant";
    // set request info
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of("invalid_tenant"));
    when(stargateRequestInfo.getCassandraToken()).thenReturn(Optional.of("test_token"));
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig);
    Field stargateRequestInfoField =
        cqlSessionCacheForTest.getClass().getDeclaredField("stargateRequestInfo");
    stargateRequestInfoField.setAccessible(true);
    stargateRequestInfoField.set(cqlSessionCacheForTest, stargateRequestInfo);
    Field fixedTokenField = cqlSessionCacheForTest.getClass().getDeclaredField("FIXED_TOKEN");
    fixedTokenField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(fixedTokenField, fixedTokenField.getModifiers() & ~Modifier.FINAL);
    fixedTokenField.set(null, fixedTenant);
    // Throwable
    Throwable t = catchThrowable(cqlSessionCacheForTest::getSession);
    assertThat(t).isNotNull().isInstanceOf(UnauthorizedException.class).hasMessage("Unauthorized");
  }
}
