package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class ProjectionABCIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final String TYPE_NAME = "Address";
  private static final String TABLE = "proj_udt";

  @BeforeAll
  public static void createTypeAndTable() {
    // Create UDT type: Address(city text, country text)
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createType(TYPE_NAME, Map.of("city", "text", "country", "text"))
        .wasSuccessful();

    // Create table with udt column
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE,
            Map.ofEntries(
                Map.entry("id", "text"),
                Map.entry("address", Map.of("type", "userDefined", "udtName", TYPE_NAME))),
            "id")
        .wasSuccessful();

    // Insert one row
    String row1 =
        """
            {
              "id": "r1",
              "address": {"city": "New York", "country": "USA"}
            }
            """;

    assertTableCommand(keyspaceName, TABLE).templated().insertOne(row1).wasSuccessful();
  }

  //
  //  @Nested
  //  class Scalars {
  //
  //    private static final String TABLE = "proj_basic";
  //
  //    @BeforeAll
  //    public static void createTable() {
  //      assertNamespaceCommand(keyspaceName)
  //          .templated()
  //          .createTable(
  //              TABLE,
  //              Map.ofEntries(
  //                  Map.entry("id", "text"),
  //                  Map.entry("name", "text"),
  //                  Map.entry("age", "int"),
  //                  Map.entry("active", "boolean")),
  //              "id")
  //          .wasSuccessful();
  //
  //      // seed a couple of rows
  //      assertTableCommand(keyspaceName, TABLE)
  //          .templated()
  //          .insertOne(
  //              """
  //              {
  //                "id": "u1",
  //                "name": "Ada",
  //                "age": 42,
  //                "active": true
  //              }
  //              """)
  //          .wasSuccessful()
  //          .hasInsertedIds(List.of("u1"));
  //
  //      assertTableCommand(keyspaceName, TABLE)
  //          .templated()
  //          .insertOne(
  //              """
  //              {
  //                "id": "u2",
  //                "name": "Bob",
  //                "age": 25,
  //                "active": false
  //              }
  //              """)
  //          .wasSuccessful()
  //          .hasInsertedIds(List.of("u2"));
  //    }
  //
  //    @Test
  //    public void inclusionProjectsOnlySelectedColumns() {
  //      // Projects only name and age
  //      assertTableCommand(keyspaceName, TABLE)
  //          .templated()
  //          .findWithExplicitProjection(
  //              Map.of("id", "u1"), Map.of("name", 1, "age", 1), Map.of(), Map.of())
  //          .wasSuccessful()
  //          .hasProjectionSchema()
  //          .hasProjectionSchemaWith("name", ApiDataTypeDefs.TEXT)
  //          .hasProjectionSchemaWith("age", ApiDataTypeDefs.INT)
  //          .doesNotHaveProjectionSchemaWith("active")
  //          .doesNotHaveProjectionSchemaWith("id")
  //          .hasDocuments(1)
  //          .hasDocumentInPosition(
  //              0,
  //              """
  //              {
  //                "name": "Ada",
  //                "age": 42
  //              }
  //              """);
  //    }
  //
  //    @Test
  //    public void noProjectionSelectsAllColumns() {
  //      // No projection clause should return all columns
  //      assertTableCommand(keyspaceName, TABLE)
  //          .templated()
  //          .findWithExplicitProjection(Map.of("id", "u1"), null, null, null)
  //          .wasSuccessful()
  //          .hasProjectionSchema()
  //          .hasProjectionSchemaWith("id", ApiDataTypeDefs.TEXT)
  //          .hasProjectionSchemaWith("name", ApiDataTypeDefs.TEXT)
  //          .hasProjectionSchemaWith("age", ApiDataTypeDefs.INT)
  //          .hasProjectionSchemaWith("active", ApiDataTypeDefs.BOOLEAN)
  //          .hasDocuments(1)
  //          .hasDocumentInPosition(
  //              0,
  //              """
  //              {
  //                "id": "u1",
  //                "name": "Ada",
  //                "age": 42,
  //                "active": true
  //              }
  //              """);
  //    }
  //
  //    @Test
  //    public void selectPrimaryKeyOnly() {
  //      // Selecting only id should return only id
  //      assertTableCommand(keyspaceName, TABLE)
  //          .templated()
  //          .findWithExplicitProjection(Map.of("id", "u2"), Map.of("id", 1), Map.of(), Map.of())
  //          .wasSuccessful()
  //          .hasProjectionSchema()
  //          .hasProjectionSchemaWith("id", ApiDataTypeDefs.TEXT)
  //          .doesNotHaveProjectionSchemaWith("name")
  //          .doesNotHaveProjectionSchemaWith("age")
  //          .doesNotHaveProjectionSchemaWith("active")
  //          .hasDocuments(1)
  //          .hasDocumentInPosition(
  //              0,
  //              """
  //              {
  //                "id": "u2"
  //              }
  //              """);
  //    }
  //
  //    @Test
  //    public void exclusionProjectsAllButExcludedColumns() {
  //      // Exclude a single column via raw projection JSON
  //      assertTableCommand(keyspaceName, TABLE)
  //          .templated()
  //          .findWithExplicitProjection(Map.of("id", "u1"), Map.of("active", 0), Map.of(),
  // Map.of())
  //          .wasSuccessful()
  //          .hasProjectionSchema()
  //          .hasProjectionSchemaWith("id", ApiDataTypeDefs.TEXT)
  //          .hasProjectionSchemaWith("name", ApiDataTypeDefs.TEXT)
  //          .hasProjectionSchemaWith("age", ApiDataTypeDefs.INT)
  //          .doesNotHaveProjectionSchemaWith("active")
  //          .hasDocuments(1)
  //          .hasDocumentInPosition(
  //              0,
  //              """
  //              {
  //                "id": "u1",
  //                "name": "Ada",
  //                "age": 42
  //              }
  //              """);
  //    }
  //  }

  @Nested
  class UdtProjectionTest {

    @Test
    public void projectUdtTopLevel() {
      // Project the entire UDT column
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .findWithExplicitProjection(Map.of("id", "r1"), Map.of("address", 1), Map.of(), Map.of())
          .wasSuccessful()
          .hasProjectionSchema()
          .hasProjectionSchemaUdt("address", TYPE_NAME)
          .hasProjectionSchemaUdtField("address", "city", "text")
          .hasProjectionSchemaUdtField("address", "country", "text")
          .hasDocuments(1)
          .hasDocumentInPosition(
              0,
              """
              {
                "address": {"city": "New York", "country": "USA"}
              }
              """);
    }

    @Test
    public void projectUdtSubField() {
      // Project a sub-field of the UDT
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .findWithExplicitProjection(
              Map.of("id", "r1"), Map.of("address.city", 1), Map.of(), Map.of())
          .wasSuccessful()
          .hasProjectionSchema()
          .hasProjectionSchemaUdt("address", TYPE_NAME)
          .hasProjectionSchemaUdtField("address", "city", "text")
          .doesNotHaveProjectionSchemaUdtField("address", "country")
          .hasDocuments(1)
          .hasDocumentInPosition(
              0,
              """
              {
                "address": {"city": "New York"}
              }
              """);
    }

    @Test
    public void projectUdtTopLevelOverridesSubfield() {
      // Selecting the top-level UDT should override any sub-field narrowing
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .findWithExplicitProjection(
              Map.of("id", "r1"), Map.of("address", 1, "address.city", 1), Map.of(), Map.of())
          .wasSuccessful()
          .hasProjectionSchema()
          .hasProjectionSchemaUdt("address", TYPE_NAME)
          .hasProjectionSchemaUdtField("address", "city", "text")
          .hasProjectionSchemaUdtField("address", "country", "text")
          .hasDocuments(1)
          .hasDocumentInPosition(
              0,
              """
              {
                "address": {"city": "New York", "country": "USA"}
              }
              """);
    }

    @Test
    public void excludeUdtSubfieldProjectsRemainingFields() {
      // Exclude a UDT sub-field; remaining UDT fields should be returned
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .findWithExplicitProjection(
              Map.of("id", "r1"), Map.of("address.city", 0), Map.of(), Map.of())
          .wasSuccessful()
          .hasProjectionSchema()
          .hasProjectionSchemaUdt("address", TYPE_NAME)
          .hasProjectionSchemaUdtField("address", "country", "text")
          .doesNotHaveProjectionSchemaUdtField("address", "city")
          .hasDocuments(1)
          .hasDocumentInPosition(
              0,
              """
              {
                "address": {"country": "USA"}
              }
              """);
    }
  }
}
