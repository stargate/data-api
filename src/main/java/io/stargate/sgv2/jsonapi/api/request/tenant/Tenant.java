package io.stargate.sgv2.jsonapi.api.request.tenant;

import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import jakarta.validation.constraints.NotNull;

import java.util.Objects;

import static io.stargate.sgv2.jsonapi.util.StringUtil.normalizeOptionalString;

public class Tenant implements Recordable {

  private static final String SINGLE_TENANT_ID = "single-tenant";

  private static final Tenant SINGLE_TENANT_ASTRA =
      new Tenant(DatabaseType.ASTRA, SINGLE_TENANT_ID, true);

  private static final Tenant SINGLE_TENANT_CASSANDRA =
      new Tenant(DatabaseType.CASSANDRA, SINGLE_TENANT_ID, true);

  private final DatabaseType databaseType;
  private final String tenantId;
  private final boolean isSingleTenant;

  private Tenant(DatabaseType databaseType, String tenantId, boolean isSingleTenant) {
    this.databaseType = databaseType;
    this.tenantId = tenantId;
    this.isSingleTenant = isSingleTenant;
  }

  public static Tenant create(DatabaseType databaseType, String tenantId) {
    Objects.requireNonNull(databaseType, "databaseType must not be null");

    var normalizedTenantId = normalizeAndValidateTenantId(databaseType, tenantId);

    if (databaseType.isSingleTenant()){
      // use switch to reduce static instance based on database type
      return switch (databaseType) {
        case ASTRA -> SINGLE_TENANT_ASTRA;
        case CASSANDRA -> SINGLE_TENANT_CASSANDRA;
        case OFFLINE_WRITER -> throw new IllegalStateException("OFFLINE_WRITER not expected");
      };
    }
    return new Tenant(databaseType, normalizedTenantId, false);
  }

  @NotNull
  public String tenantId() {
    return tenantId;
  }

  public DatabaseType databaseType() {
    return databaseType;
  }

   static String normalizeAndValidateTenantId(DatabaseType databaseType, String tenantId) {

     var normalized = normalizeOptionalString(tenantId);

     if (databaseType.isSingleTenant()) {
       if (!normalized.isBlank()) {
         throw new IllegalArgumentException(
             "tenantId must be null or blank, got '" + normalized + "' for database type " + databaseType);
       }
       return SINGLE_TENANT_ID;
     }

     if (normalized.isBlank()) {
       throw new IllegalArgumentException(
           "tenantId must not be null or blank for database type " + databaseType);
     }
     return normalized;
  }

  @Override
  public String toString() {
    return Objects.toString(tenantId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseType, tenantId);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Tenant that)) {
      return false;
    }
    return Objects.equals(databaseType, that.databaseType) && Objects.equals(tenantId, that.tenantId);
  }

  @Override
  public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    return dataRecorder
        .append("tenantId", tenantId)
        .append("databaseType", databaseType);
  }
}
