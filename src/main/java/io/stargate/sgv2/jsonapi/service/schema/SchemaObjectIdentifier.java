package io.stargate.sgv2.jsonapi.service.schema;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;
import static io.stargate.sgv2.jsonapi.util.StringUtil.normalizeOptionalString;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.logging.LoggingMDCContext;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.slf4j.MDC;

public class SchemaObjectIdentifier implements KeyspaceScopedName, Recordable, LoggingMDCContext {

  private final SchemaObjectType type;
  private final Tenant tenant;
  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;

  private final String fullName;

  private SchemaObjectIdentifier(
      SchemaObjectType type, Tenant tenant, CqlIdentifier keyspace, CqlIdentifier table) {

    this.type = type;
    this.tenant = tenant;
    this.keyspace = keyspace;
    this.table = table;

    this.fullName =
        table == null
            ? cqlIdentifierToMessageString(keyspace)
            : cqlIdentifierToMessageString(keyspace) + "." + cqlIdentifierToMessageString(table);
  }

  public static SchemaObjectIdentifier forDatabase(Tenant tenant) {

    checkTeantId(tenant);
    return new SchemaObjectIdentifier(SchemaObjectType.DATABASE, tenant, null, null);
  }

  public static SchemaObjectIdentifier forKeyspace(Tenant tenant, CqlIdentifier keyspace) {

    checkTeantId(tenant);
    checkKeyspaceName(keyspace);
    return new SchemaObjectIdentifier(SchemaObjectType.KEYSPACE, tenant, keyspace, null);
  }

  public static SchemaObjectIdentifier forCollection(
      Tenant tenant, CqlIdentifier keyspace, CqlIdentifier collection) {

    checkTeantId(tenant);
    checkKeyspaceName(keyspace);
    checkTableName("collection", collection);
    return new SchemaObjectIdentifier(SchemaObjectType.COLLECTION, tenant, keyspace, collection);
  }

  public static SchemaObjectIdentifier forTable(
      Tenant tenant, CqlIdentifier keyspace, CqlIdentifier table) {

    checkTeantId(tenant);
    checkKeyspaceName(keyspace);
    checkTableName("table", table);
    return new SchemaObjectIdentifier(SchemaObjectType.TABLE, tenant, keyspace, table);
  }

  public static SchemaObjectIdentifier fromTableMetadata(
      SchemaObjectType type, Tenant tenant, TableMetadata tableMetadata) {

    checkTeantId(tenant);
    Objects.requireNonNull(tableMetadata, "tableMetadata must not be null");

    return switch (type) {
      case TABLE -> forTable(tenant, tableMetadata.getKeyspace(), tableMetadata.getName());
      case COLLECTION ->
          forCollection(tenant, tableMetadata.getKeyspace(), tableMetadata.getName());
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

  @Override
  @Nullable
  public CqlIdentifier keyspace() {
    return keyspace;
  }

  @Nullable
  public CqlIdentifier table() {
    return table;
  }

  @Override
  public CqlIdentifier objectName() {
    return table;
  }

  public boolean isSameKeyspace(SchemaObjectIdentifier other) {
    return Objects.equals(tenant, other.tenant) && Objects.equals(keyspace, other.keyspace);
  }

  private static void checkTeantId(Tenant tenant) {
    Objects.requireNonNull(tenant, "tenant name must not be null");
  }

  private static void checkKeyspaceName(CqlIdentifier keyspace) {
    Objects.requireNonNull(keyspace, "keyspace name must not be null");
    Preconditions.checkArgument(
        !keyspace.asInternal().isBlank(), "keyspace name must not be blank");
  }

  private static void checkTableName(String context, CqlIdentifier table) {
    Objects.requireNonNull(table, context + " name must not be null");
    Preconditions.checkArgument(!table.asInternal().isBlank(), context + " name must not be blank");
  }

  @Override
  public void addToMDC() {
    // NOTE: MUST stay as namespace for logging analysis
    MDC.put("namespace", keyspace.asInternal());

    // NOTE: MUST stay as collection for logging analysis
    MDC.put("collection", normalizeOptionalString( table == null ? null : table.asInternal()));
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
        && Objects.equals(keyspace, that.keyspace)
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
