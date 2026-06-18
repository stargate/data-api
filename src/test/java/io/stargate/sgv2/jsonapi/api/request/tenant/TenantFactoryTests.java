package io.stargate.sgv2.jsonapi.api.request.tenant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.sgv2.jsonapi.config.DatabaseType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TenantFactoryTests {

  @AfterEach
  public void resetSingleton() {
    TenantFactory.reset();
  }

  @Test
  public void shouldInitializeOnce() {
    TenantFactory.initialize(DatabaseType.ASTRA);
    TenantFactory instance = TenantFactory.instance();

    assertThat(instance).isNotNull();
  }

  @Test
  public void shouldFailIfInitializedTwice() {
    TenantFactory.initialize(DatabaseType.ASTRA);

    assertThatThrownBy(() -> TenantFactory.initialize(DatabaseType.CASSANDRA))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already initialized");
  }

  @Test
  public void shouldFailIfAccessedBeforeInit() {
    assertThatThrownBy(TenantFactory::instance)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not initialized");
  }

  @Test
  public void shouldCreateTenantWithCorrectDatabaseType() {
    TenantFactory.initialize(DatabaseType.ASTRA);
    TenantFactory factory = TenantFactory.instance();

    Tenant tenant = factory.create("abc");

    assertThat(tenant).isNotNull();
    assertThat(tenant.databaseType()).isEqualTo(DatabaseType.ASTRA);
    assertThat(tenant.toString()).isEqualTo("abc");
  }

  @Test
  public void shouldCreateTenantWithRegion() {
    TenantFactory.initialize(DatabaseType.ASTRA);
    TenantFactory factory = TenantFactory.instance();

    Tenant tenant = factory.create("abc", "us-west-2");

    assertThat(tenant.region()).isEqualTo("us-west-2");
  }

  @Test
  public void shouldIgnoreTenantIdAndRegionForCassandra() {
    // CASSANDRA is single-tenant: the factory ignores both arguments and always returns the
    // built-in Cassandra singleton with its fixed region.
    TenantFactory.initialize(DatabaseType.CASSANDRA);
    TenantFactory factory = TenantFactory.instance();

    Tenant tenant = factory.create("ignored-tenant-id", "ignored-region");

    assertThat(tenant.databaseType()).isEqualTo(DatabaseType.CASSANDRA);
    assertThat(tenant.region()).isEqualTo(Tenant.CASSANDRA_REGION_DEFAULT);
  }

  @Test
  public void shouldDefaultUnknownRegionWhenNotProvided() {
    TenantFactory.initialize(DatabaseType.ASTRA);
    TenantFactory factory = TenantFactory.instance();

    Tenant tenant = factory.create("abc");

    assertThat(tenant.region()).isEqualTo(Tenant.UNKNOWN_REGION);
  }
}
