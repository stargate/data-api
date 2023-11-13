package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.util.Map;

public class TenantAwareCqlSessionBuilder extends CqlSessionBuilder {
  private static final String TENANT_ID_PROPERTY_KEY = "TENANT_ID";
  private final String tenantId;

  public TenantAwareCqlSessionBuilder(String tenantId) {
    if (tenantId == null || tenantId.isEmpty()) {
      throw new RuntimeException("Tenant ID cannot be null or empty");
    }
    this.tenantId = tenantId;
  }

  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    return new TenantAwareDriverContext(tenantId, configLoader, programmaticArguments);
  }

  public static class TenantAwareDriverContext extends DefaultDriverContext {
    private final String tenantId;

    public TenantAwareDriverContext(
        String tenantId,
        DriverConfigLoader configLoader,
        ProgrammaticArguments programmaticArguments) {
      super(configLoader, programmaticArguments);
      this.tenantId = tenantId;
    }

    @Override
    protected Map<String, String> buildStartupOptions() {
      Map<String, String> existing = super.buildStartupOptions();
      return NullAllowingImmutableMap.<String, String>builder(existing.size() + 1)
          .putAll(existing)
          .put(TENANT_ID_PROPERTY_KEY, tenantId)
          .build();
    }
  }
}
