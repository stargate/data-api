package io.stargate.sgv2.jsonapi.service.schema;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.logging.LoggingMDCContext;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.slf4j.MDC;

public class SchemaObjectIdentifier implements Recordable, LoggingMDCContext {

  private final SchemaObjectType type;
  private final Tenant tenant;
  private final String keyspace;
  private final String table;

  private final String fullName;

  private SchemaObjectIdentifier(
      SchemaObjectType type, Tenant tenant, String keyspace, String table) {
    this.type = type;
    this.tenant = tenant;
    this.keyspace = keyspace;
    this.table = table;

    this.fullName = table == null ? keyspace : keyspace + "." + table;
  }

  public static SchemaObjectIdentifier forDatabase(Tenant tenant) {
    checkTeantId(tenant);
    return new SchemaObjectIdentifier(SchemaObjectType.DATABASE, tenant, null, null);
  }

  public static SchemaObjectIdentifier forKeyspace(Tenant tenant, String keyspace) {
    checkTeantId(tenant);
    checkKeyspaceName(keyspace);
    return new SchemaObjectIdentifier(SchemaObjectType.KEYSPACE, tenant, keyspace, null);
  }

  // factory for a collection
  public static SchemaObjectIdentifier forCollection(
      Tenant tenant, String keyspace, String collection) {
    checkTeantId(tenant);
    checkKeyspaceName(keyspace);
    checkTableName("collection", collection);
    return new SchemaObjectIdentifier(SchemaObjectType.COLLECTION, tenant, keyspace, collection);
  }

  // factory for a table
  public static SchemaObjectIdentifier forTable(Tenant tenant, String keyspace, String table) {
    checkTeantId(tenant);
    checkKeyspaceName(keyspace);
    checkTableName("table", table);
    return new SchemaObjectIdentifier(SchemaObjectType.TABLE, tenant, keyspace, table);
  }

  // fatopry takes TableMetadata and type
  public static SchemaObjectIdentifier fromTableMetadata(
      SchemaObjectType type, Tenant tenant, TableMetadata tableMetadata) {

    checkTeantId(tenant);
    Objects.requireNonNull(tableMetadata, "tableMetadata must not be null");

    return switch (type) {
      case TABLE ->
          forTable(
              tenant,
              tableMetadata.getKeyspace().asInternal(),
              tableMetadata.getName().asInternal());
      case COLLECTION ->
          forCollection(
              tenant,
              tableMetadata.getKeyspace().asInternal(),
              tableMetadata.getName().asInternal());
      default -> throw new IllegalArgumentException("Unsupported type: " + type);
    };
  }

  public SchemaObjectType type() {
    return type;
  }

  public String fullName() {
    return fullName;
  }

  public Tenant tenant() {
    return tenant;
  }

  public String keyspace() {
    return keyspace;
  }

  @Nullable
  public String table() {
    return table;
  }

  public boolean isSameKeyspace(SchemaObjectIdentifier other) {
    return Objects.equals(tenant, other.tenant) && Objects.equals(keyspace, other.keyspace);
  }

  private static void checkTeantId(Tenant tenant) {
    Objects.requireNonNull(tenant, "tenant name must not be null");
  }

  private static void checkKeyspaceName(String keyspace) {
    Objects.requireNonNull(keyspace, "keyspace name must not be null");
    Preconditions.checkArgument(!keyspace.isBlank(), "keyspace name must not be blank");
  }

  private static void checkTableName(String context, String table) {
    Objects.requireNonNull(table, context + " name must not be null");
    Preconditions.checkArgument(!table.isBlank(), context + " name must not be blank");
  }

  @Override
  public void addToMDC() {
    // NOTE: MUST stay as namespace for logging analysis
    MDC.put("namespace", keyspace);

    // NOTE: MUST stay as collection for logging analysis
    MDC.put("collection", table);
  }

  @Override
  public void removeFromMDC() {
    MDC.remove("namespace");
    MDC.remove("collection");
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SchemaObjectIdentifier that)) {
      return false;
    }
    return type == that.type
        && Objects.equals(tenant, that.tenant)
        && keyspace.equals(that.keyspace)
        && Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, tenant, keyspace, table);
  }

  @Override
  public String toString() {
    return PrettyPrintable.print(this);
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("tenant", tenant)
        .append("type", type)
        .append("keyspace", keyspace)
        .append("table", table);
  }

}
