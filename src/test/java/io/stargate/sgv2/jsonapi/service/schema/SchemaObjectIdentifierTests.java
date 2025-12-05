package io.stargate.sgv2.jsonapi.service.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.util.recordable.Jsonable;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

public class SchemaObjectIdentifierTests {

  protected final TestConstants TEST_CONSTANTS = new TestConstants();
  private final Tenant OTHER_TENANT = Tenant.create(TEST_CONSTANTS.DATABASE_TYPE, "other-tenant-" + TEST_CONSTANTS.CORRELATION_ID);

  @Test
  public void forDatabaseFactory() {

    assertThatThrownBy(() -> SchemaObjectIdentifier.forDatabase(null))
        .as("throws if tenant is null")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tenant");

    var tenant = TEST_CONSTANTS.TENANT;
    var identifier = SchemaObjectIdentifier.forDatabase(tenant);
    assertThat(identifier.fullName())
        .as("fullName should be db:<tenant>")
        .isEqualTo("db:" + tenant);

    assertThat(identifier.type())
        .as("type should be DATABASE")
        .isEqualTo(SchemaObjectType.DATABASE);

    assertThat(identifier.tenant())
        .as("tenant should match the one used to create the identifier")
        .isEqualTo(tenant);
  }

  @Test
  public void forKeyspaceFactory() {
    var tenant = TEST_CONSTANTS.TENANT;

    assertThatThrownBy(() -> SchemaObjectIdentifier.forKeyspace(null, CqlIdentifier.fromCql("ks")))
        .as("throws if tenant is null")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tenant");

    assertThatThrownBy(() -> SchemaObjectIdentifier.forKeyspace(tenant, null))
        .as("throws if keyspace is null")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("keyspace");

    // need fromInternal to create a blank CqlIdentifier
    assertThatThrownBy(() -> SchemaObjectIdentifier.forKeyspace(tenant, CqlIdentifier.fromInternal("")))
        .as("throws if keyspace is blank")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("keyspace name must not be blank");

    var keyspace = CqlIdentifier.fromCql("ks");
    var identifier = SchemaObjectIdentifier.forKeyspace(tenant, keyspace);
    assertThat(identifier.fullName())
        .as("fullName should equal keyspace name")
        .isEqualTo("ks");

    assertThat(identifier.type())
        .as("type should be KEYSPACE")
        .isEqualTo(SchemaObjectType.KEYSPACE);

    assertThat(identifier.tenant())
        .as("tenant should match the one used to create the identifier")
        .isEqualTo(tenant);
  }

  @Test
  public void forCollectionFactory() {
    var tenant = TEST_CONSTANTS.TENANT;
    var keyspace = CqlIdentifier.fromCql("ks");

    assertThatThrownBy(() -> SchemaObjectIdentifier.forCollection(null, keyspace, CqlIdentifier.fromCql("col")))
        .as("throws if tenant is null")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tenant");

    assertThatThrownBy(() -> SchemaObjectIdentifier.forCollection(tenant, null, CqlIdentifier.fromCql("col")))
        .as("throws if keyspace is null")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("keyspace");

    assertThatThrownBy(() -> SchemaObjectIdentifier.forCollection(tenant, keyspace, null))
        .as("throws if collection is null")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("collection");

    // need fromInternal to create a blank CqlIdentifier
    assertThatThrownBy(() -> SchemaObjectIdentifier.forCollection(tenant, keyspace, CqlIdentifier.fromInternal("")))
        .as("throws if collection is blank")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("collection name must not be blank");

    var collection = CqlIdentifier.fromCql("col");
    var identifier = SchemaObjectIdentifier.forCollection(tenant, keyspace, collection);
    assertThat(identifier.fullName())
        .as("fullName should be keyspace.collection")
        .isEqualTo("ks.col");

    assertThat(identifier.type())
        .as("type should be COLLECTION")
        .isEqualTo(SchemaObjectType.COLLECTION);

    assertThat(identifier.tenant())
        .as("tenant should match the one used to create the identifier")
        .isEqualTo(tenant);
  }

  @Test
  public void forTableFactory() {
    var tenant = TEST_CONSTANTS.TENANT;
    var keyspace = CqlIdentifier.fromCql("ks");

    assertThatThrownBy(() -> SchemaObjectIdentifier.forTable(null, keyspace, CqlIdentifier.fromCql("tbl")))
        .as("throws if tenant is null")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tenant");

    assertThatThrownBy(() -> SchemaObjectIdentifier.forTable(tenant, null, CqlIdentifier.fromCql("tbl")))
        .as("throws if keyspace is null")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("keyspace");

    assertThatThrownBy(() -> SchemaObjectIdentifier.forTable(tenant, keyspace, null))
        .as("throws if table is null")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("table");

    // need fromInternal to create a blank CqlIdentifier
    assertThatThrownBy(() -> SchemaObjectIdentifier.forTable(tenant, keyspace, CqlIdentifier.fromInternal("")))
        .as("throws if table is blank")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("table name must not be blank");

    var table = CqlIdentifier.fromCql("tbl");
    var identifier = SchemaObjectIdentifier.forTable(tenant, keyspace, table);
    assertThat(identifier.fullName())
        .as("fullName should be keyspace.table")
        .isEqualTo("ks.tbl");

    assertThat(identifier.type())
        .as("type should be TABLE")
        .isEqualTo(SchemaObjectType.TABLE);

    assertThat(identifier.tenant())
        .as("tenant should match the one used to create the identifier")
        .isEqualTo(tenant);
  }


  @Test
  public void fromTableMetadata() {
    var tenant = TEST_CONSTANTS.TENANT;
    var keyspace = CqlIdentifier.fromCql("ks");
    var table = CqlIdentifier.fromCql("tbl");

    var metadata = mock(TableMetadata.class);
    when(metadata.getKeyspace()).thenReturn(keyspace);
    when(metadata.getName()).thenReturn(table);

    var fromTable = SchemaObjectIdentifier.fromTableMetadata(SchemaObjectType.TABLE, tenant, metadata);
    assertThat(fromTable.type())
        .as("should return SchemaObjectType.TABLE")
        .isEqualTo(SchemaObjectType.TABLE);
    assertThat(fromTable.fullName())
        .as("should extract fullName from metadata for TABLE")
        .isEqualTo("ks.tbl");

    var fromCollection = SchemaObjectIdentifier.fromTableMetadata(SchemaObjectType.COLLECTION, tenant, metadata);
    assertThat(fromCollection.type())
        .as("should return SchemaObjectType.COLLECTION")
        .isEqualTo(SchemaObjectType.COLLECTION);
    assertThat(fromCollection.fullName())
        .as("should extract fullName from metadata for COLLECTION")
        .isEqualTo("ks.tbl");

    assertThatThrownBy(() ->
        SchemaObjectIdentifier.fromTableMetadata(SchemaObjectType.KEYSPACE, tenant, metadata))
        .as("should throw for unsupported SchemaObjectType")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported object type");

    assertThatThrownBy(() ->
        SchemaObjectIdentifier.fromTableMetadata(SchemaObjectType.TABLE, tenant, null))
        .as("should throw if TableMetadata is null")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tableMetadata must not be null");
  }

  @Test
  public void keyspaceIdentifierIsExpected() {
    var tenant = TEST_CONSTANTS.TENANT;
    var keyspace = CqlIdentifier.fromCql("ks");
    var table = CqlIdentifier.fromCql("tbl");

    var tableIdentifier = SchemaObjectIdentifier.forTable(tenant, keyspace, table);
    var keyspaceIdentifier = tableIdentifier.keyspaceIdentifier();

    assertThat(keyspaceIdentifier.type())
        .as("should return type KEYSPACE")
        .isEqualTo(SchemaObjectType.KEYSPACE);

    assertThat(keyspaceIdentifier.keyspace())
        .as("keyspaceIdentifier has same keyspace as the table identifier")
        .isEqualTo(tableIdentifier.keyspace());
    assertThat(keyspaceIdentifier.tenant())
        .as("keyspaceIdentifier has same tenant as the table identifier")
        .isEqualTo(tableIdentifier.tenant());
  }

  @Test
  public void unscopedIdentifierInterface() {
    var tenant = TEST_CONSTANTS.TENANT;
    var keyspace = CqlIdentifier.fromCql("ks");
    var table = CqlIdentifier.fromCql("tbl");

    var tableIdentifier = SchemaObjectIdentifier.forTable(tenant, keyspace, table);
    var unscoped = (UnscopedSchemaObjectIdentifier)tableIdentifier;

    assertThat(unscoped.keyspace())
        .as("unscoped keyspace should match the table identifier's keyspace")
        .isEqualTo(tableIdentifier.keyspace());

    assertThat(unscoped.objectName())
        .as("unscoped object name should match the table identifier's table name")
        .isEqualTo(tableIdentifier.table());
  }

  @Test
  public void toStringIsFullName() {
    var tenant = TEST_CONSTANTS.TENANT;
    var keyspace = CqlIdentifier.fromCql("ks");
    var table = CqlIdentifier.fromCql("tbl");

    var tableIdentifier = SchemaObjectIdentifier.forTable(tenant, keyspace, table);

    assertThat(tableIdentifier.toString())
        .as("toString should return fullName")
        .isEqualTo(tableIdentifier.fullName());
  }

  @Test
  public void isSameKeyspace() {
    var tenant = TEST_CONSTANTS.TENANT;
    var keyspace = CqlIdentifier.fromCql("ks");

    var table1 = CqlIdentifier.fromCql("t1");
    var table2 = CqlIdentifier.fromCql("t2");

    var id1 = SchemaObjectIdentifier.forTable(tenant, keyspace, table1);
    var id2 = SchemaObjectIdentifier.forTable(tenant, keyspace, table2);

    assertThat(id1.isSameKeyspace(id2))
        .as("should return true when tenant and keyspace match")
        .isTrue();

    var id3 = SchemaObjectIdentifier.forTable(OTHER_TENANT, keyspace, table1);

    assertThat(id1.isSameKeyspace(id3))
        .as("should return false when tenant differs")
        .isFalse();

    var otherKeyspace = CqlIdentifier.fromCql("otherks");
    var id4 = SchemaObjectIdentifier.forTable(tenant, otherKeyspace, table1);

    assertThat(id1.isSameKeyspace(id4))
        .as("should return false when keyspace differs")
        .isFalse();
  }

  @Test
  public void testAddAndRemoveFromMDC() {
    var tenant = TEST_CONSTANTS.TENANT;
    var keyspace = CqlIdentifier.fromCql("ks");
    var table = CqlIdentifier.fromCql("tbl");

    var identifier = SchemaObjectIdentifier.forTable(tenant, keyspace, table);

    identifier.addToMDC();

    assertThat(MDC.get("namespace"))
        .as("MDC should contain keyspace as 'namespace'")
        .isEqualTo("ks");

    assertThat(MDC.get("collection"))
        .as("MDC should contain table as 'collection'")
        .isEqualTo("tbl");

    identifier.removeFromMDC();

    assertThat(MDC.get("namespace"))
        .as("MDC 'namespace' should be removed")
        .isNull();

    assertThat(MDC.get("collection"))
        .as("MDC 'collection' should be removed")
        .isNull();
  }

  @Test
  public void recordTo(){
    var tenant = TEST_CONSTANTS.TENANT;
    var keyspace = CqlIdentifier.fromCql("ks");
    var table = CqlIdentifier.fromCql("tbl");

    var identifier = SchemaObjectIdentifier.forTable(tenant, keyspace, table);

    var pretty = PrettyPrintable.pprint(identifier);
    assertThat(pretty)
        .as("recordTo output for SchemaObjectIdentifier")
        .contains("tenant", tenant.toString())
        .contains("databaseType", TEST_CONSTANTS.DATABASE_TYPE.name())
        .contains("type", "TABLE")
        .contains("keyspace", "ks")
        .contains("table", "tbl");

    var identifierJson = Jsonable.toJson(identifier);
    var expected = JsonNodeFactory.instance.objectNode();
    // there is a top level "SchemaObjectIdentifier" field
    var contents = expected.withObjectProperty("SchemaObjectIdentifier");
    contents.put("tenant",Jsonable.toJson(tenant));
    contents.put("type", "TABLE");
    contents.put("keyspace", keyspace.asInternal());
    contents.put("table", table.asInternal());

    assertThat(identifierJson)
        .as("JSON output for Tenant")
        .isEqualTo(expected);
  }

  @Test
  public void equalityAndHashCodeReflexive() {
    var db = TEST_CONSTANTS.DATABASE_IDENTIFIER;
    var ks = TEST_CONSTANTS.KEYSPACE_IDENTIFIER;
    var coll = TEST_CONSTANTS.COLLECTION_IDENTIFIER;
    var table = TEST_CONSTANTS.TABLE_IDENTIFIER;

    assertThat(db).isEqualTo(db).hasSameHashCodeAs(db);
    assertThat(ks).isEqualTo(ks).hasSameHashCodeAs(ks);
    assertThat(coll).isEqualTo(coll).hasSameHashCodeAs(coll);
    assertThat(table).isEqualTo(table).hasSameHashCodeAs(table);
  }

  @Test
  public void equalityAndHashCodeTransitive() {
    var id1 = SchemaObjectIdentifier.forCollection(TEST_CONSTANTS.TENANT,
        TEST_CONSTANTS.KEYSPACE_IDENTIFIER.keyspace(),
        TEST_CONSTANTS.COLLECTION_IDENTIFIER.table());

    var id2 = SchemaObjectIdentifier.forCollection(TEST_CONSTANTS.TENANT,
        TEST_CONSTANTS.KEYSPACE_IDENTIFIER.keyspace(),
        TEST_CONSTANTS.COLLECTION_IDENTIFIER.table());

    var id3 = SchemaObjectIdentifier.forCollection(TEST_CONSTANTS.TENANT,
        TEST_CONSTANTS.KEYSPACE_IDENTIFIER.keyspace(),
        TEST_CONSTANTS.COLLECTION_IDENTIFIER.table());

    assertThat(id1).isEqualTo(id2);
    assertThat(id2).isEqualTo(id3);
    assertThat(id1).isEqualTo(id3);

    assertThat(id1.hashCode()).isEqualTo(id2.hashCode());
    assertThat(id2.hashCode()).isEqualTo(id3.hashCode());
  }

  @Test
  public void differentTenant() {
    var id1 = SchemaObjectIdentifier.forCollection(TEST_CONSTANTS.TENANT,
        TEST_CONSTANTS.KEYSPACE_IDENTIFIER.keyspace(),
        TEST_CONSTANTS.COLLECTION_IDENTIFIER.table());

    var id2 = SchemaObjectIdentifier.forCollection(OTHER_TENANT,
        TEST_CONSTANTS.KEYSPACE_IDENTIFIER.keyspace(),
        TEST_CONSTANTS.COLLECTION_IDENTIFIER.table());

    assertThat(id1).isNotEqualTo(id2);

    assertThat(id1.hashCode()).isNotEqualTo(id2.hashCode());
  }
}