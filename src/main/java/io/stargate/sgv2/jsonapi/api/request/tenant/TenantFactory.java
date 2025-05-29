package io.stargate.sgv2.jsonapi.api.request.tenant;

import io.stargate.sgv2.jsonapi.config.DatabaseType;

import java.util.Objects;

/**
 * Singleton instance factory for creating {@link Tenant} instances. It must be initialized
 * at application startup by calling {@link #initialize(DatabaseType)} with the
 * database type from the configuration.
 * <p>
 * Then call {@link #create(String)} on the {@link #instance()} to create tenant instances.
 * <p>
 * Safe for multi threading.
 */
public class TenantFactory {

  private static TenantFactory singleton;

  private final DatabaseType databaseType;

  private TenantFactory(DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  public static void initialize(DatabaseType databaseType) {
    Objects.requireNonNull(databaseType, "databaseType must not be null");

    if (singleton == null) {
      singleton = new TenantFactory(databaseType);
    }
    else{
      throw new IllegalStateException("TenantFactory already initialized");
    }
  }

  public static TenantFactory instance() {
    if (singleton == null) {
      throw new IllegalStateException("TenantIdFactory not initialized");
    }
    return singleton;
  }

  public Tenant create(String tenantId) {
    return Tenant.create(databaseType, tenantId);
  }
}
