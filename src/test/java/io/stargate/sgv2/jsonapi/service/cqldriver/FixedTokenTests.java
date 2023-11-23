package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.service.cqldriver.TenantAwareCqlSessionBuilderTest.TENANT_ID_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.quarkus.security.UnauthorizedException;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.util.Optional;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(FixedTokenOverrideProfile.class)
public class FixedTokenTests {
  private static final String TENANT_ID_FOR_TEST = "test_tenant";

  @Inject OperationsConfig operationsConfig;

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
    UniAssertSubscriber<CqlSession> subscriber = cqlSessionCacheForTest.getSession()
                    .subscribe().withSubscriber(UniAssertSubscriber.create());
    assertThat(
            ((DefaultDriverContext) subscriber.awaitItem().assertItem(cqlSession -> cqlSession.getContext())
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
    // Throwable
    Throwable t = catchThrowable(cqlSessionCacheForTest::getSession);
    assertThat(t).isNotNull().isInstanceOf(UnauthorizedException.class).hasMessage("Unauthorized");
  }
}
