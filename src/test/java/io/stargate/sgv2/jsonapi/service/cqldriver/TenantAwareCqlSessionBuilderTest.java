package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.AssertionsForClassTypes.*;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class TenantAwareCqlSessionBuilderTest {
  private static final String TEST_TENANT_ID = "95816830-7dec-11ee-b962-0242ac120002";
  protected static final String TENANT_ID_PROPERTY_KEY = "TENANT_ID";

  @Test
  public void testTenantAwareCqlSessionBuilderTenant() {
    TenantAwareCqlSessionBuilder tenantAwareCqlSessionBuilder =
        new TenantAwareCqlSessionBuilder(TEST_TENANT_ID);
    DriverConfigLoader driverConfigLoader = new DefaultDriverConfigLoader();
    ProgrammaticArguments programmaticArguments = ProgrammaticArguments.builder().build();
    DriverContext driverContext =
        tenantAwareCqlSessionBuilder.buildContext(driverConfigLoader, programmaticArguments);
    assertThat(driverContext)
        .isInstanceOf(TenantAwareCqlSessionBuilder.TenantAwareDriverContext.class);
    assertThat(
            ((TenantAwareCqlSessionBuilder.TenantAwareDriverContext) driverContext)
                .getStartupOptions()
                .get(TENANT_ID_PROPERTY_KEY))
        .isEqualTo(TEST_TENANT_ID);
  }

  @Test
  public void testTenantAwareCqlSessionBuilderNullTenant() {
    Throwable t = catchThrowable(() -> new TenantAwareCqlSessionBuilder(null));
    assertThat(t)
        .isNotNull()
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Tenant ID cannot be null or empty");
  }
}
