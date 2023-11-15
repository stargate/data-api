package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.util.Map;

/**
 * This is an extension of the {@link CqlSessionBuilder} that allows to pass a tenant ID to the
 * CQLSession via TenantAwareDriverContext which is an extension of the {@link DefaultDriverContext}
 * that adds the tenant ID to the startup options.
 */
public class TenantAwareCqlSessionBuilder extends CqlSessionBuilder {
  /**
   * Property key that will be used to pass the tenant ID to the CQLSession via
   * TenantAwareDriverContext
   */
  private static final String TENANT_ID_PROPERTY_KEY = "TENANT_ID";
  /** Tenant ID that will be passed to the CQLSession via TenantAwareDriverContext */
  private final String tenantId;

  /**
   * Constructor that takes the tenant ID as a parameter
   *
   * @param tenantId tenant id or database id
   */
  public TenantAwareCqlSessionBuilder(String tenantId) {
    if (tenantId == null || tenantId.isEmpty()) {
      throw new RuntimeException("Tenant ID cannot be null or empty");
    }
    this.tenantId = tenantId;
  }

  /**
   * Overridden method that builds the custom driver context
   *
   * @param configLoader configuration loader
   * @param programmaticArguments programmatic arguments
   * @return custom driver context
   */
  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    return new TenantAwareDriverContext(tenantId, configLoader, programmaticArguments);
  }

  /**
   * This is an extension of the {@link DefaultDriverContext} that adds the tenant ID to the startup
   * options.
   */
  public static class TenantAwareDriverContext extends DefaultDriverContext {
    /** Tenant ID that will be added to the startup options */
    private final String tenantId;

    /**
     * Constructor that takes the tenant ID as a parameter
     *
     * @param tenantId tenant id or database id
     * @param configLoader configuration loader
     * @param programmaticArguments programmatic arguments
     */
    public TenantAwareDriverContext(
        String tenantId,
        DriverConfigLoader configLoader,
        ProgrammaticArguments programmaticArguments) {
      super(configLoader, programmaticArguments);
      this.tenantId = tenantId;
    }

    /**
     * Overridden method that adds the tenant ID to the startup options with the key {@link
     * TenantAwareCqlSessionBuilder#TENANT_ID_PROPERTY_KEY}
     *
     * @return startup options
     */
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
