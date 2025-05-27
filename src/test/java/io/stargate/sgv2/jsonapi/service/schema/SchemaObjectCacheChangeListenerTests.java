package io.stargate.sgv2.jsonapi.service.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.github.benmanes.caffeine.cache.Ticker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.api.request.tenant.TenantFactory;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.CacheTestsBase;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link SchemaObjectCache.SchemaCacheSchemaChangeListener} to evict and clear items
 * see XXX for tests on the cache directly.
 */
public class SchemaObjectCacheChangeListenerTests extends CacheTestsBase {

    private final UserAgent SLA_USER_AGENT = new UserAgent("user-agent/" + TEST_CONSTANTS.CORRELATION_ID);

    @Before
    public void setUp() {
        // the listener needs to create tenants, and it uses this factory
      TenantFactory.initialize(TEST_CONSTANTS.DATABASE_TYPE);
    }

    @Test
    public void silentFailWhenOnSessionReadyNotCalled() {

      var fixture = newFixture();
      var expectedTable = fixture.mockTable(TEST_CONSTANTS.TABLE_IDENTIFIER, TEST_CONSTANTS.USER_AGENT) ;

      var tableMetadata = fixture.tableMetadataForIdentifier(TEST_CONSTANTS.TABLE_IDENTIFIER);
      var keyspaceMetadata = fixture.keyspaceMetadataForIdentifier(TEST_CONSTANTS.TABLE_IDENTIFIER);

      // if the listener is called before onSessionReady is called it should not error

      fixture.schemaChangeListener.onTableDropped(tableMetadata);
      fixture.schemaChangeListener.onTableCreated(tableMetadata);
      fixture.schemaChangeListener.onTableUpdated(tableMetadata, tableMetadata);

      fixture.schemaChangeListener.onKeyspaceDropped(keyspaceMetadata);
      fixture.schemaChangeListener.onKeyspaceCreated(keyspaceMetadata);
      fixture.schemaChangeListener.onKeyspaceUpdated(keyspaceMetadata, keyspaceMetadata);

      // and the table should still be there
      var actualTableAfter = fixture.cache()
          .getIfPresent(fixture.requestContext, TEST_CONSTANTS.TABLE_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
      assertThat(actualTableAfter)
          .as("Table is still in schema cache after listener called before onSessionReady")
          .isPresent()
          .get()
          .isSameAs(expectedTable);
    }

    @Test
    public void tableChangesEvictTable() {

      var otherTenant = Tenant.create(DatabaseType.ASTRA, "other-tenant-" + TEST_CONSTANTS.CORRELATION_ID);

      var table1Identifier = TEST_CONSTANTS.TABLE_IDENTIFIER;
      var table2Identifier = SchemaObjectIdentifier.forTable(
          TEST_CONSTANTS.TENANT, table1Identifier.keyspace(), CqlIdentifier.fromInternal("table2"));
      // NOTE: actual table name is the same as table1Identifier, but in a different tenant !
      var table3Identifier = SchemaObjectIdentifier.forTable(
          otherTenant,
          table1Identifier.keyspace(), table1Identifier.table());

      var tableMetadata = newFixture().tableMetadataForIdentifier(table1Identifier);

      List<Function<SchemaChangeListener, String>> calls =
          List.of(
              (cb) -> {
                cb.onTableCreated(tableMetadata);
                return "onTableCreated";
              },
              (cb) -> {
                cb.onTableUpdated(tableMetadata, tableMetadata);
                return "onTableUpdated";
              },
              (cb) -> {
                cb.onTableDropped(tableMetadata);
                return "onTableDropped";
              });

      for (var cb : calls) {
        var fixture = newFixture();

        // put two tables in from two keyspaces for the tenant we are removing
        // and one from a different tenant
        // table 1 is the one we remove
        var expectedTable1 = fixture.mockTable(table1Identifier, TEST_CONSTANTS.USER_AGENT);
        var expectedTable2 = fixture.mockTable(table2Identifier, TEST_CONSTANTS.USER_AGENT);
        var expectedTable3 = fixture.mockTable(table3Identifier, TEST_CONSTANTS.USER_AGENT);

        fixture.schemaChangeListener.onSessionReady(fixture.cqlSession);
        var operation = cb.apply(fixture.schemaChangeListener);

        // only table1 should be removed, the others should still be there
        assertThat(fixture.cache.getIfPresent(fixture.requestContext, table1Identifier, TEST_CONSTANTS.USER_AGENT))
            .as("%s removed on table event operation=%s", table1Identifier, operation)
            .isEmpty();

        assertThat(fixture.cache.getIfPresent(fixture.requestContext, table2Identifier, TEST_CONSTANTS.USER_AGENT))
            .as("%s present on table event operation=%s", table2Identifier, operation)
            .isEmpty();

        assertThat(fixture.cache.getIfPresent(fixture.requestContext, table3Identifier, TEST_CONSTANTS.USER_AGENT))
            .as("%s present on table event operation=%s", table3Identifier, operation)
            .isEmpty();
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

      Assertions.assertThat(fixture.schemaCache.peekSchemaObject(TENANT_ID, "keyspace1",
   "table1"))
          .as("TENANT_ID keyspace1 removed")
          .isEmpty();
      Assertions.assertThat(fixture.schemaCache.peekSchemaObject(TENANT_ID, "keyspace2",
   "table2"))
          .as("TENANT_ID keyspace2 not removed")
          .contains(table2);
      Assertions.assertThat(fixture.schemaCache.peekSchemaObject(notTenantId, "keyspace3",
   "table3"))
          .as("notTenantId keyspace3 not removed")
          .contains(table3);
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
        SchemaObjectCache.SchemaObjectFactory schemaObjectFactory,
        SchemaChangeListener schemaChangeListener,
        SchemaObjectCache cache,
        Ticker ticker,
        RequestContext requestContext,
        CqlSession cqlSession) {


     public TableSchemaObject mockTable(SchemaObjectIdentifier identifier, UserAgent userAgent) {

       var tableSchemaObject = mock(TableSchemaObject.class);

       when(tableSchemaObject.identifier()).thenReturn(identifier);
       when(schemaObjectFactory.apply(any(), eq(identifier), any()))
           .thenReturn(CompletableFuture.completedFuture(tableSchemaObject));

       addToCache(identifier, userAgent, tableSchemaObject);
       return tableSchemaObject;
     }

     public void addToCache(SchemaObjectIdentifier identifier, UserAgent userAgent, TableSchemaObject tableSchemaObject) {

       var actualTable = cache()
           .getTableBased(requestContext, identifier, userAgent, false)
           .await().indefinitely();

       assertThat(actualTable)
           .as("Table is one returned by the factory")
           .isEqualTo(tableSchemaObject);
     }

     public TableMetadata tableMetadataForIdentifier(SchemaObjectIdentifier identifier) {

       var tableMetaData =  mock(TableMetadata.class);
        when(tableMetaData.getKeyspace()).thenReturn(identifier.keyspace());
        when(tableMetaData.getName()).thenReturn(identifier.table());
        return tableMetaData;
     }

      public KeyspaceMetadata keyspaceMetadataForIdentifier(SchemaObjectIdentifier identifier) {

        var keyspaceMetaData = mock(KeyspaceMetadata.class);
        when(keyspaceMetaData.getName()).thenReturn(identifier.keyspace());
        return keyspaceMetaData;
      }
   }


    private Fixture newFixture() {

      var schemaObjectFactory = mock(SchemaObjectCache.SchemaObjectFactory.class);
      var ticker = new CacheTestsBase.FakeTicker();

      var requestContext = mock(RequestContext.class);

      var cqlSession = mock(CqlSession.class);
      when(cqlSession.getName()).thenReturn(TEST_CONSTANTS.TENANT.toString());

      var cache = new SchemaObjectCache(
          CACHE_MAX_SIZE,
          LONG_TTL,
          SLA_USER_AGENT,
          SHORT_TTL,
          schemaObjectFactory,
          new SimpleMeterRegistry());

      return new Fixture(
          schemaObjectFactory,
          cache.getSchemaChangeListener(),
          cache,
          ticker,
          requestContext,
          cqlSession);
    }
}
