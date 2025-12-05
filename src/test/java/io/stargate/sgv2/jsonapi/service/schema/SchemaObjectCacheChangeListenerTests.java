package io.stargate.sgv2.jsonapi.service.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

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
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.CacheTestsBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;


/**
 * Tests for the {@link SchemaObjectCache.SchemaCacheSchemaChangeListener} to evict and clear items
 * see XXX for tests on the cache directly.
 */
public class SchemaObjectCacheChangeListenerTests extends CacheTestsBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaObjectCacheChangeListenerTests.class);


  private final UserAgent SLA_USER_AGENT =
      new UserAgent("user-agent/" + TEST_CONSTANTS.CORRELATION_ID);

  private final Tenant OTHER_TENANT =
      Tenant.create(DatabaseType.ASTRA, "other-tenantFixture-" + TEST_CONSTANTS.CORRELATION_ID);

  private final SchemaObjectIdentifier TABLE_1_IDENTIFIER = TEST_CONSTANTS.TABLE_IDENTIFIER;

  private final SchemaObjectIdentifier TABLE_2_IDENTIFIER =
      SchemaObjectIdentifier.forTable(
          TEST_CONSTANTS.TENANT,
          TABLE_1_IDENTIFIER.keyspace(),
          CqlIdentifier.fromInternal("table2"));

  // NOTE: actual table name is the same as TABLE_1_IDENTIFIER, but in a different tenant!
  private final SchemaObjectIdentifier TABLE_OTHER_IDENTIFIER =
      SchemaObjectIdentifier.forTable(
          OTHER_TENANT, TABLE_1_IDENTIFIER.keyspace(), TABLE_1_IDENTIFIER.table());

  private final SchemaObjectIdentifier KEYSPACE_1_IDENTIFIER =
      TABLE_1_IDENTIFIER.keyspaceIdentifier();

  private final SchemaObjectIdentifier KEYSPACE_OTHER_IDENTIFIER =
      TABLE_OTHER_IDENTIFIER.keyspaceIdentifier();

  @BeforeEach
  public void setUp() {
    // the listener needs to create tenants, and it uses this factory
    TenantFactory.initialize(TEST_CONSTANTS.DATABASE_TYPE);
  }

  @AfterEach
  public void reset() {
    TenantFactory.reset();
  }


  @Test
  public void silentFailWhenOnSessionReadyNotCalled() {

    var fixture = newFixture();
    var expectedTable =
        fixture.mockTable(fixture.tenantFixture, TEST_CONSTANTS.TABLE_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);

    var tableMetadata = fixture.tableMetadataForIdentifier(TEST_CONSTANTS.TABLE_IDENTIFIER);
    var keyspaceMetadata = fixture.keyspaceMetadataForIdentifier(TEST_CONSTANTS.TABLE_IDENTIFIER);

    // if the listener is called before onSessionReady is called it should not error
    fixture.listener.onTableDropped(tableMetadata);
    fixture.listener.onTableCreated(tableMetadata);
    fixture.listener.onTableUpdated(tableMetadata, tableMetadata);

    fixture.listener.onKeyspaceDropped(keyspaceMetadata);
    fixture.listener.onKeyspaceCreated(keyspaceMetadata);
    fixture.listener.onKeyspaceUpdated(keyspaceMetadata, keyspaceMetadata);

    // and the table should still be there
    var actualTableAfter =
        fixture
            .cache()
            .getIfPresent(
                fixture.tenantFixture.requestContext, TEST_CONSTANTS.TABLE_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);

    assertThat(actualTableAfter)
        .as("Table is still in schema cache after listener called before onSessionReady")
        .isPresent()
        .get()
        .isSameAs(expectedTable);
  }

  @Test
  public void tableChangesEvictTable() {

    var tableMetadata = newFixture().tableMetadataForIdentifier(TABLE_1_IDENTIFIER);

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

      // put two tables in from two keyspaces for the tenantFixture we are removing
      // and one from a different tenantFixture
      // table 1 is the one we remove
      var expectedTable1 = fixture.mockTable(fixture.tenantFixture, TABLE_1_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
      var expectedTable2 = fixture.mockTable(fixture.tenantFixture, TABLE_2_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
      var expectedTableOther = fixture.mockTable(fixture.otherTenantFixture, TABLE_OTHER_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);

      fixture.listener.onSessionReady(fixture.tenantFixture().cqlSession);
      var operation = cb.apply(fixture.listener);

      // only table1 should be removed, the others should still be there
      assertSchemaObjectRemoved(operation,fixture.tenantFixture, fixture, TABLE_1_IDENTIFIER);
      assertSchemaObjectPresent(operation, fixture.tenantFixture, fixture, TABLE_2_IDENTIFIER, expectedTable2);
      assertSchemaObjectPresent(operation,fixture.otherTenantFixture,  fixture, TABLE_OTHER_IDENTIFIER, expectedTableOther);
    }
  }

  @Test
  public void keyspaceDroppedEvictsAllForKS() {

    var fixture = newFixture();

    // put two tables in from two keyspaces for the tenantFixture we are removing
    // and one from a different tenantFixture
    // table 1 and 2 is the ones we remove
    var expectedTable1 = fixture.mockTable(fixture.tenantFixture,TABLE_1_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
    var expectedTable2 = fixture.mockTable(fixture.tenantFixture,TABLE_2_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
    var expectedTableOther = fixture.mockTable(fixture.otherTenantFixture, TABLE_OTHER_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);

    var expectedKS = fixture.mockKeyspace(fixture.tenantFixture, KEYSPACE_1_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
    var expectedKSOther =
        fixture.mockKeyspace(fixture.otherTenantFixture, KEYSPACE_OTHER_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);

    var ksMetadata1 = fixture.keyspaceMetadataForIdentifier(KEYSPACE_1_IDENTIFIER);

    // the cql session name is set for to use the TEST_CONSTANTS.TENANT
    fixture.listener.onSessionReady(fixture.tenantFixture.cqlSession);
    // drop keyspace 1, from the TENANT
    fixture.listener.onKeyspaceDropped(ksMetadata1);

    // table1 and 2 should be removed because from same keyspace + tenantFixture, table other still there
    // keyspace 1 should be removed, but not the other keyspace
    var operation = "onKeyspaceDropped";
    assertSchemaObjectRemoved(operation,fixture.tenantFixture, fixture, TABLE_1_IDENTIFIER);
    assertSchemaObjectRemoved(operation,fixture.tenantFixture, fixture, TABLE_2_IDENTIFIER);
    assertSchemaObjectPresent(operation,fixture.otherTenantFixture, fixture, TABLE_OTHER_IDENTIFIER, expectedTableOther);

    assertSchemaObjectRemoved(operation,fixture.tenantFixture, fixture, KEYSPACE_1_IDENTIFIER);
    assertSchemaObjectPresent(operation, fixture.otherTenantFixture, fixture, KEYSPACE_OTHER_IDENTIFIER, expectedKSOther);
  }

  @Test
  public void keyspaceUpdatedEvictsOnlyKs() {

    var fixture = newFixture();

    // put two tables in from two keyspaces for the tenantFixture we are removing
    // and one from a different tenantFixture
    var expectedTable1 = fixture.mockTable(fixture.tenantFixture,TABLE_1_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
    var expectedTable2 = fixture.mockTable(fixture.tenantFixture,TABLE_2_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
    var expectedTableOther = fixture.mockTable(fixture.otherTenantFixture, TABLE_OTHER_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);

    var expectedKS = fixture.mockKeyspace(fixture.tenantFixture, KEYSPACE_1_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
    var expectedKSOther =
        fixture.mockKeyspace(fixture.otherTenantFixture, KEYSPACE_OTHER_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);

    var ksMetadata1 = fixture.keyspaceMetadataForIdentifier(KEYSPACE_1_IDENTIFIER);

    // the cql session name is set for to use the TEST_CONSTANTS.TENANT
    fixture.listener.onSessionReady(fixture.tenantFixture.cqlSession);
    // drop keyspace 1, from the TENANT
    fixture.listener.onKeyspaceUpdated(ksMetadata1, ksMetadata1);

    // only ks for the tenant should be removed, other tables and ks should still be there
    var operation = "onKeyspaceUpdated";
    assertSchemaObjectPresent(operation,fixture.tenantFixture, fixture, TABLE_1_IDENTIFIER, expectedTable1);
    assertSchemaObjectPresent(operation,fixture.tenantFixture, fixture, TABLE_2_IDENTIFIER, expectedTable2);
    assertSchemaObjectPresent(operation,fixture.otherTenantFixture, fixture, TABLE_OTHER_IDENTIFIER, expectedTableOther);

    assertSchemaObjectRemoved(operation,fixture.tenantFixture, fixture, KEYSPACE_1_IDENTIFIER);
    assertSchemaObjectPresent(operation, fixture.otherTenantFixture, fixture, KEYSPACE_OTHER_IDENTIFIER, expectedKSOther);
  }

  @Test
  public void keyspaceCreatedEvictsAllForKS() {

    var fixture = newFixture();


    var expectedTable1 = fixture.mockTable(fixture.tenantFixture,TABLE_1_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
    var expectedTable2 = fixture.mockTable(fixture.tenantFixture,TABLE_2_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
    var expectedTableOther = fixture.mockTable(fixture.otherTenantFixture, TABLE_OTHER_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);

    var expectedKS = fixture.mockKeyspace(fixture.tenantFixture, KEYSPACE_1_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);
    var expectedKSOther =
        fixture.mockKeyspace(fixture.otherTenantFixture, KEYSPACE_OTHER_IDENTIFIER, TEST_CONSTANTS.USER_AGENT);

    var ksMetadata1 = fixture.keyspaceMetadataForIdentifier(KEYSPACE_1_IDENTIFIER);

    // the cql session name is set for to use the TEST_CONSTANTS.TENANT
    fixture.listener.onSessionReady(fixture.tenantFixture.cqlSession);
    // drop keyspace 1, from the TENANT
    fixture.listener.onKeyspaceCreated(ksMetadata1);

    // only ks for the tenant should be removed, other tables and ks should still be there
    var operation = "onKeyspaceCreated";
    assertSchemaObjectRemoved(operation,fixture.tenantFixture, fixture, TABLE_1_IDENTIFIER);
    assertSchemaObjectRemoved(operation,fixture.tenantFixture, fixture, TABLE_2_IDENTIFIER);
    assertSchemaObjectPresent(operation,fixture.otherTenantFixture, fixture, TABLE_OTHER_IDENTIFIER, expectedTableOther);

    assertSchemaObjectRemoved(operation,fixture.tenantFixture, fixture, KEYSPACE_1_IDENTIFIER);
    assertSchemaObjectPresent(operation, fixture.otherTenantFixture, fixture, KEYSPACE_OTHER_IDENTIFIER, expectedKSOther);
  }

  // =========================================================================================================
  // Helper methods to create mock objects for testing
  // =========================================================================================================

  private void assertSchemaObjectRemoved(
      String operation, FixtureTenant thisFixtureTenant, Fixture fixture, SchemaObjectIdentifier identifier) {

    assertThat(
            fixture.cache.getIfPresent(
                thisFixtureTenant.requestContext, identifier, TEST_CONSTANTS.USER_AGENT))
        .as("%s removed on after operation=%s", identifier, operation)
        .isEmpty();
  }

  private void assertSchemaObjectPresent(
      String operation,
      FixtureTenant thisFixtureTenant,
      Fixture fixture,
      SchemaObjectIdentifier identifier,
      SchemaObject expectedSchemaObject) {
    assertThat(
            fixture.cache.getIfPresent(
                thisFixtureTenant.requestContext, identifier, TEST_CONSTANTS.USER_AGENT))
        .as("%s present on after operation=%s", identifier, operation)
        .isPresent()
        .get()
        .isSameAs(expectedSchemaObject);
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

  private Fixture newFixture() {

    var schemaObjectFactory = mock(SchemaObjectCache.SchemaObjectFactory.class);
    var ticker = new CacheTestsBase.FakeTicker();

    var cache =
        new SchemaObjectCache(
            CACHE_MAX_SIZE,
            LONG_TTL,
            SLA_USER_AGENT,
            SHORT_TTL,
            schemaObjectFactory,
            new SimpleMeterRegistry(),
            true,
            ticker);

    return new Fixture(
        schemaObjectFactory,
        cache.getSchemaChangeListener(),
        cache,
        ticker,
        newFixtureTenant(TEST_CONSTANTS.TENANT),
        newFixtureTenant(OTHER_TENANT));
  }

  private FixtureTenant newFixtureTenant(Tenant tenant) {

    var requestContext = mock(RequestContext.class);
    when(requestContext.tenant()).thenReturn(tenant);

    var cqlSession = mock(CqlSession.class);
    when(cqlSession.getName()).thenReturn(tenant.toString());

    return new FixtureTenant(tenant, requestContext, cqlSession);
  }
  record Fixture(
      SchemaObjectCache.SchemaObjectFactory factory,
      SchemaChangeListener listener,
      SchemaObjectCache cache,
      Ticker ticker,
      FixtureTenant tenantFixture,
      FixtureTenant otherTenantFixture) {

    public TableSchemaObject mockTable(FixtureTenant thisFixtureTenant, SchemaObjectIdentifier identifier, UserAgent userAgent) {

      var tableSchemaObject = mock(TableSchemaObject.class);
      when(tableSchemaObject.identifier()).thenReturn(identifier);

      addToCache(thisFixtureTenant, identifier, userAgent, tableSchemaObject);
      return tableSchemaObject;
    }

    public KeyspaceSchemaObject mockKeyspace(
        FixtureTenant thisFixtureTenant, SchemaObjectIdentifier identifier, UserAgent userAgent) {

      var keyspaceSchemaObject = mock(KeyspaceSchemaObject.class);

      when(keyspaceSchemaObject.identifier()).thenReturn(identifier);

      addToCache(thisFixtureTenant, identifier, userAgent, keyspaceSchemaObject);
      return keyspaceSchemaObject;
    }

    public void addToCache(
        FixtureTenant thisFixtureTenant,
        SchemaObjectIdentifier identifier,
        UserAgent userAgent,
        TableSchemaObject tableSchemaObject) {

      LOGGER.info("Setting up factories for identifier: {}, thisFixtureTenant: {}", identifier, thisFixtureTenant);

      // the getTableBased function may need to make two calls to get the schema object, the first
      // is to try if it is a collection, the second is to get it as a table
      // so we need ot make sure to throw an exception when called

      // this is the success call
      when(factory.apply(any(), eq(identifier), anyBoolean()))
          .thenReturn(CompletableFuture.completedFuture(tableSchemaObject));

      // now the failure call
      var otherIdentifier =
          identifier.type() == SchemaObjectType.COLLECTION
              ? SchemaObjectIdentifier.forTable(
              identifier.tenant(), identifier.keyspace(), identifier.table())
              : SchemaObjectIdentifier.forCollection(
              identifier.tenant(), identifier.keyspace(), identifier.table());
      when(factory.apply(any(), eq(otherIdentifier), anyBoolean()))
          .thenReturn(
              CompletableFuture.failedFuture(
                  new SchemaObjectFactory.SchemaObjectTypeMismatchException(
                      identifier.type(), otherIdentifier.type())));

      LOGGER.info("Calling cache to get table for identifier: {}", identifier);

      var actualTable =
          cache()
              .getTableBased(thisFixtureTenant.requestContext, identifier, userAgent, false)
              .await()
              .indefinitely();

      assertThat(actualTable)
          .as(
              "Table is one returned by the factory, expected:%s actual:%s",
              identifier, actualTable.identifier())
          .isEqualTo(tableSchemaObject);
    }

    public void addToCache(
        FixtureTenant thisFixtureTenant,
        SchemaObjectIdentifier identifier,
        UserAgent userAgent,
        KeyspaceSchemaObject keyspaceSchemaObject) {

      when(factory.apply(any(), eq(identifier), anyBoolean()))
          .thenReturn(CompletableFuture.completedFuture(keyspaceSchemaObject));

      var actual =
          cache().getKeyspace(thisFixtureTenant.requestContext, identifier, userAgent, false)
              .await()
              .indefinitely();

      assertThat(actual)
          .as("Keyspace is one returned by the factory")
          .isEqualTo(keyspaceSchemaObject);
    }

    public TableMetadata tableMetadataForIdentifier(SchemaObjectIdentifier identifier) {

      var tableMetaData = mock(TableMetadata.class);
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

  record FixtureTenant(
      Tenant tenant,
      RequestContext requestContext,
      CqlSession cqlSession
  ){}
}
