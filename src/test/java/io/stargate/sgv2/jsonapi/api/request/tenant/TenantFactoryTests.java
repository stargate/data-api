package io.stargate.sgv2.jsonapi.api.request.tenant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.sgv2.jsonapi.config.DatabaseType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

public class TenantFactoryTests {

  @AfterEach
  public void resetSingleton() throws Exception {
    // Reset the private static singleton field via reflection to isolate tests
    Field field = TenantFactory.class.getDeclaredField("singleton");
    field.setAccessible(true);
    field.set(null, null);
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
    assertThat(tenant.toString()).isEqualTo("ABC");
  }
}