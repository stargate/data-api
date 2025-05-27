package io.stargate.sgv2.jsonapi.api.request.tenant;

import io.stargate.sgv2.jsonapi.config.DatabaseType;

public class TenantFactory {

  private static TenantFactory singleton;

  private final DatabaseType databaseType;

  private TenantFactory(DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  public static void initialize(DatabaseType databaseType) {
    if (singleton == null) {
      singleton = new TenantFactory(databaseType);
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
