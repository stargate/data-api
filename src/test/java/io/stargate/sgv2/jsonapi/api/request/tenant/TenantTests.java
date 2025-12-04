package io.stargate.sgv2.jsonapi.api.request.tenant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.util.recordable.Jsonable;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import org.junit.jupiter.api.Test;

/** Tests for the {@link Tenant} class */
public class TenantTests {

  private static final String SINGLE_TENANT_ID = "SINGLE-TENANT";

  @Test
  public void astraDbRequiresTenantID() {

    assertThrows(
        IllegalArgumentException.class,
        () -> Tenant.create(DatabaseType.ASTRA, null),
        "ASTRA DB throws on null tenant ID");

    assertThrows(
        IllegalArgumentException.class,
        () -> Tenant.create(DatabaseType.ASTRA, ""),
        "ASTRA DB throws on empty tenant ID");

    assertThrows(
        IllegalArgumentException.class,
        () -> Tenant.create(DatabaseType.ASTRA, "   "),
        "ASTRA DB throws on blank tenant ID");
  }

  @Test
  public void astraDbCreation() {

    var lowercase = Tenant.create(DatabaseType.ASTRA, "aa11zz");
    assertThat(lowercase.databaseType()).isEqualTo(DatabaseType.ASTRA);
    assertThat(lowercase.toString()).isEqualTo("AA11ZZ");
  }

  @Test
  public void cassandraDbRejectsTenantId() {

    assertThrows(
        IllegalArgumentException.class,
        () -> Tenant.create(DatabaseType.CASSANDRA, "aa"),
        "Cassandra DB throws on non blank tenant ID");
  }

  @Test
  public void cassandraDbCreation() {

    var nullTenant = Tenant.create(DatabaseType.CASSANDRA, null);
    assertThat(nullTenant.databaseType()).isEqualTo(DatabaseType.CASSANDRA);
    assertThat(nullTenant.toString()).isEqualTo(SINGLE_TENANT_ID);

    var emptyTenant = Tenant.create(DatabaseType.CASSANDRA, "");
    assertThat(nullTenant.toString()).isEqualTo(SINGLE_TENANT_ID);

    var blankTenant = Tenant.create(DatabaseType.CASSANDRA, "   ");
    assertThat(blankTenant.toString()).isEqualTo(SINGLE_TENANT_ID);
  }

  @Test
  public void equalsAndHashCode() {
    var tenantA = Tenant.create(DatabaseType.ASTRA, "Tenant-123");
    var tenantB = Tenant.create(DatabaseType.ASTRA, "tenant-123");
    var tenantC = Tenant.create(DatabaseType.ASTRA, "TENANT-123");

    // Reflexivity
    assertThat(tenantA).isEqualTo(tenantA);

    // Symmetry
    assertThat(tenantA).isEqualTo(tenantB);
    assertThat(tenantB).isEqualTo(tenantA);

    // Transitivity
    assertThat(tenantA).isEqualTo(tenantB);
    assertThat(tenantB).isEqualTo(tenantC);
    assertThat(tenantA).isEqualTo(tenantC);

    // Hash code consistency
    assertThat(tenantA.hashCode()).isEqualTo(tenantB.hashCode());
    assertThat(tenantB.hashCode()).isEqualTo(tenantC.hashCode());
  }

  @Test
  public void notEqualDifferentDbType() {
    var tenant1 = Tenant.create(DatabaseType.ASTRA, SINGLE_TENANT_ID);
    var tenant2 = Tenant.create(DatabaseType.CASSANDRA, null);

    assertThat(tenant1).isNotEqualTo(tenant2);
    assertThat(tenant1.hashCode()).isNotEqualTo(tenant2.hashCode());
  }

  @Test
  public void recordTo() {

    var tenant = Tenant.create(DatabaseType.ASTRA, "Tenant-123");

    assertThat(PrettyPrintable.pprint(tenant))
        .as("Recording to pretty print")
        .contains("tenantId", "TENANT-123")
        .contains("databaseType", DatabaseType.ASTRA.name());

    var tenantJson = Jsonable.toJson(tenant);
    var expected = JsonNodeFactory.instance.objectNode();
    // Top level node with the class name
    var contents = expected.withObjectProperty("Tenant");
    contents.put("tenantId", "TENANT-123");
    contents.put("databaseType", DatabaseType.ASTRA.name());

    assertThat(tenantJson).as("Recording to JSON").isEqualTo(expected);
  }
}
