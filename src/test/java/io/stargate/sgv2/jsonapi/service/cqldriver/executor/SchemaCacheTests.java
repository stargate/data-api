package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.junit.Test;

public class SchemaCacheTests {

  private static final String TENANT_ID = "tenant-id" + System.currentTimeMillis();

  @Test
  public void deactivatedTenantRemovesAllKeyspaces() {

    var notTenantId = "not-tenant-id";
    var fixture = newFixture();

    // put two tables in from two keyspaces
    var table1 = addTable(fixture, TENANT_ID, "keyspace1", "table1");
    var table2 = addTable(fixture, TENANT_ID, "keyspace2", "table2");
    var table3 = addTable(fixture, notTenantId, "keyspace3", "table3");

    var deactivatedListener = fixture.schemaCache.getDeactivatedTenantConsumer();

    deactivatedListener.accept(TENANT_ID);

    // all of TENANT_ID should be removed, and the non TENANT_ID can stay
    assertThat(fixture.schemaCache.peekSchemaObject(TENANT_ID, "keyspace1", "table1"))
        .as("TENANT_ID keyspace1 removed")
        .isEmpty();
    assertThat(fixture.schemaCache.peekSchemaObject(TENANT_ID, "keyspace2", "table2"))
        .as("TENANT_ID keyspace2 removed")
        .isEmpty();
    assertThat(fixture.schemaCache.peekSchemaObject(notTenantId, "keyspace3", "table3"))
        .as("notTenantId keyspace3 not removed")
        .contains(table3);
  }

  @Test
  public void schemasChangeListenerNotInitialized() {

    var fixture = newFixture();
    var table1 = addTable(fixture, TENANT_ID, "keyspace1", "table1");

    var listener = fixture.schemaCache.getSchemaChangeListener();

    // if the listener is called before onSessionReady is called it should not error
    var tableMetadata = mock(TableMetadata.class);
    listener.onTableDropped(tableMetadata);
    listener.onTableCreated(tableMetadata);
    listener.onTableUpdated(tableMetadata, tableMetadata);

    var keyspaceMetadata = mock(KeyspaceMetadata.class);
    listener.onKeyspaceDropped(keyspaceMetadata);

    // table1 should still be in the cache even because listener does not know what tenant
    // the session was for
    assertThat(fixture.schemaCache.peekSchemaObject(TENANT_ID, "keyspace1", "table1"))
        .as("TENANT_ID keyspace1 not removed")
        .contains(table1);
  }

  @Test
  public void tableChangesEvictTabe() {

    var removedTableMetadata = tableMetadata("keyspace1", "table1");

    List<Function<SchemaChangeListener, String>> calls =
        List.of(
            (cb) -> {
              cb.onTableCreated(removedTableMetadata);
              return "onTableCreated";
            },
            (cb) -> {
              cb.onTableUpdated(removedTableMetadata, removedTableMetadata);
              return "onTableUpdated";
            },
            (cb) -> {
              cb.onTableDropped(removedTableMetadata);
              return "onTableDropped";
            });
    var notTenantId = "not-tenant-id";

    for (var cb : calls) {
      var fixture = newFixture();

      // put two tables in from two keyspaces for the tenant we are removing
      var table1 = addTable(fixture, TENANT_ID, "keyspace1", "table1");
      var table2 = addTable(fixture, TENANT_ID, "keyspace2", "table2");
      var table3 = addTable(fixture, notTenantId, "keyspace3", "table3");

      var listener = fixture.schemaCache.getSchemaChangeListener();
      listener.onSessionReady(fixture.session);
      var operation = cb.apply(listener);

      // all of TENANT_ID should be removed, and the non TENANT_ID can stay
      assertThat(fixture.schemaCache.peekSchemaObject(TENANT_ID, "keyspace1", "table1"))
          .as("TENANT_ID keyspace1 removed on table event operation=%s", operation)
          .isEmpty();
      assertThat(fixture.schemaCache.peekSchemaObject(TENANT_ID, "keyspace2", "table2"))
          .as("TENANT_ID keyspace2 not removed on table event operation=%s", operation)
          .contains(table2);
      assertThat(fixture.schemaCache.peekSchemaObject(notTenantId, "keyspace3", "table3"))
          .as("notTenantId keyspace3 not removed on table event operation=%s", operation)
          .contains(table3);
    }
  }

  @Test
  public void keyspaceDroppedEvictsAllTables() {

    var notTenantId = "not-tenant-id";
    var fixture = newFixture();

    var table1 = addTable(fixture, TENANT_ID, "keyspace1", "table1");
    var table2 = addTable(fixture, TENANT_ID, "keyspace2", "table2");
    var table3 = addTable(fixture, notTenantId, "keyspace3", "table3");

    var listener = fixture.schemaCache.getSchemaChangeListener();
    listener.onSessionReady(fixture.session);
    listener.onKeyspaceDropped(keyspaceMetadata("keyspace1"));

    assertThat(fixture.schemaCache.peekSchemaObject(TENANT_ID, "keyspace1", "table1"))
        .as("TENANT_ID keyspace1 removed")
        .isEmpty();
    assertThat(fixture.schemaCache.peekSchemaObject(TENANT_ID, "keyspace2", "table2"))
        .as("TENANT_ID keyspace2 not removed")
        .contains(table2);
    assertThat(fixture.schemaCache.peekSchemaObject(notTenantId, "keyspace3", "table3"))
        .as("notTenantId keyspace3 not removed")
        .contains(table3);
  }

  /** Add a keyspace and cache item to the schema cache. */
  private SchemaObject addTable(
      Fixture fixture, String tenantId, String keyspaceName, String tableName) {

    // setup to return a mocked table schema object
    var tableSchemaCache = mock(TableBasedSchemaCache.class);
    var expectedSchemaObject = mock(CollectionSchemaObject.class);

    when(tableSchemaCache.getSchemaObject(any(RequestContext.class), eq(tableName), anyBoolean()))
        .thenReturn(Uni.createFrom().item(() -> expectedSchemaObject));
    when(tableSchemaCache.peekSchemaObject(eq(tableName)))
        .thenReturn(Optional.of(expectedSchemaObject));

    // override so that we return an empty schema object after it was evicted
    doAnswer(
            invocation -> {
              when(tableSchemaCache.peekSchemaObject(eq(tableName))).thenReturn(Optional.empty());
              return null;
            })
        .when(tableSchemaCache)
        .evictCollectionSettingCacheEntry(tableName);

    when(fixture.tableCacheFactory.create(eq(keyspaceName), any(), any()))
        .thenReturn(tableSchemaCache);

    // request context only needs the few things the SchemaCache uses
    var requestContext = mock(RequestContext.class);
    when(requestContext.getTenantId()).thenReturn(Optional.of(tenantId));

    // setup so we return the  TableBasedSchemaCache for the keyspace
    reset(fixture.tableCacheFactory);
    when(fixture.tableCacheFactory.create(eq(keyspaceName), any(), any()))
        .thenReturn(tableSchemaCache);

    var actualUni =
        fixture.schemaCache.getSchemaObject(requestContext, keyspaceName, tableName, false);

    assertThat(actualUni).as("getSchemaObject returns non null").isNotNull();
    var actualSchemaObject = actualUni.await().indefinitely();
    assertThat(actualSchemaObject)
        .as("getSchemaObject returns the schema object from table cache")
        .isSameAs(expectedSchemaObject);

    assertThat(fixture.schemaCache.peekSchemaObject(tenantId, keyspaceName, tableName))
        .as("peekSchemaObject returns the schema object from table cache")
        .contains(expectedSchemaObject);

    return actualSchemaObject;
  }

  private TableMetadata tableMetadata(String keyspaceName, String tableName) {

    var tableMetadata = mock(TableMetadata.class);

    var keyspaceIdentifier = mock(CqlIdentifier.class);
    when(keyspaceIdentifier.asInternal()).thenReturn(keyspaceName);
    when(tableMetadata.getKeyspace()).thenReturn(keyspaceIdentifier);

    var tableIdentifier = mock(CqlIdentifier.class);
    when(tableIdentifier.asInternal()).thenReturn(tableName);
    when(tableMetadata.getName()).thenReturn(tableIdentifier);

    return tableMetadata;
  }

  private KeyspaceMetadata keyspaceMetadata(String keyspaceName) {

    var keyspaceMetadata = mock(KeyspaceMetadata.class);

    var keyspaceIdentifier = mock(CqlIdentifier.class);
    when(keyspaceIdentifier.asInternal()).thenReturn(keyspaceName);
    when(keyspaceMetadata.getName()).thenReturn(keyspaceIdentifier);

    return keyspaceMetadata;
  }

  record Fixture(
      Session session, SchemaCache.TableCacheFactory tableCacheFactory, SchemaCache schemaCache) {}

  private Fixture newFixture() {
    return newFixture(DatabaseType.ASTRA);
  }

  private Fixture newFixture(DatabaseType databaseType) {

    var session = mock(Session.class);
    when(session.getName()).thenReturn(TENANT_ID);

    var tableCacheFactory = mock(SchemaCache.TableCacheFactory.class);

    var sessionCacheSupplier = mock(CqlSessionCacheSupplier.class);
    when(sessionCacheSupplier.get()).thenReturn(mock(CQLSessionCache.class));

    var objectMapper = mock(ObjectMapper.class);

    var databaseConfig = mock(OperationsConfig.DatabaseConfig.class);
    var operationsConfig = mock(OperationsConfig.class);
    when(operationsConfig.databaseConfig()).thenReturn(databaseConfig);
    when(databaseConfig.type()).thenReturn(databaseType);

    SchemaCache schemaCache =
        new SchemaCache(sessionCacheSupplier, objectMapper, operationsConfig, tableCacheFactory);
    return new Fixture(session, tableCacheFactory, schemaCache);
  }
}
