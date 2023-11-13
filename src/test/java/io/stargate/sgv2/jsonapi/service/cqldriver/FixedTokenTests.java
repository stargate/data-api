package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.service.cqldriver.TenantAwareCqlSessionBuilderTest.TENANT_ID_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.quarkus.security.UnauthorizedException;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class FixedTokenTests {
  private static final String FIXED_TOKEN_FOR_TEST = "fixed_token";
  private static final String TENANT_ID_FOR_TEST = "test_tenant";

  @InjectMock protected StargateRequestInfo stargateRequestInfo;

  @Inject CQLSessionCache cqlSessionCache;

  @Inject OperationsConfig operationsConfig;

  @BeforeAll
  public static void setup() {
    System.setProperty(CQLSessionCache.FIXED_TOKEN_PROPERTY_NAME, FIXED_TOKEN_FOR_TEST);
  }

  @AfterAll
  public static void teardown() {
    System.clearProperty(CQLSessionCache.FIXED_TOKEN_PROPERTY_NAME);
  }

  @Test
  public void testOSSCxCQLSessionCacheWithFixedToken()
      throws NoSuchFieldException, IllegalAccessException {
    // set request info
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
    when(stargateRequestInfo.getCassandraToken()).thenReturn(Optional.of(FIXED_TOKEN_FOR_TEST));
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

  // @Disabled("TODO: fix this test")
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
    // Throwable
    Throwable t = catchThrowable(cqlSessionCacheForTest::getSession);
    assertThat(t).isNotNull().isInstanceOf(UnauthorizedException.class).hasMessage("Unauthorized");
  }
}
