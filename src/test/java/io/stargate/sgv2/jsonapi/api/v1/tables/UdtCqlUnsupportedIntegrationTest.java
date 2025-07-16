package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Data API unsupported UDT created by CQL user */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
public class UdtCqlUnsupportedIntegrationTest extends AbstractTableIntegrationTestBase {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(UdtCqlUnsupportedIntegrationTest.class);

  private static void logCql(String cql) {
    LOGGER.info("Asserting CQL executes: {}", cql);
  }

  private static String tableName(String typeName) {
    return "table_for_udt_" + typeName;
  }

  private static String udtColName(String typeName) {
    // using dbl quotes because typeName is snakeCase
    return "\"col_for_type_" + typeName + "\"";
  }

  private void assertDropTable(String tableName) {
    var dropTable =
            """
    DROP TABLE IF EXISTS "%s"."%s";
    """
            .formatted(keyspaceName, tableName);
    logCql(dropTable);
    assertThat(executeCqlStatement(dropTable)).isTrue();
  }

  private void assertDropType(String typeName) {
    var dropType =
            """
          DROP TYPE IF EXISTS "%s"."%s";
        """
            .formatted(keyspaceName, typeName);
    logCql(dropType);
    assertThat(executeCqlStatement(dropType)).isTrue();
  }

  private void assertCreateType(String typeName, String fields) {
    var createType =
            """
        CREATE TYPE "%s"."%s" %s;
      """
            .formatted(keyspaceName, typeName, fields);
    logCql(createType);
    assertThat(executeCqlStatement(createType)).isTrue();
  }

  private void assertCreateTableForUdt(String typeName, String columnType) {
    var createTable =
            """
        CREATE TABLE "%s"."%s" (
            id text PRIMARY KEY,
            %s %s
        );
        """
            .formatted(keyspaceName, tableName(typeName), udtColName(typeName), columnType);
    logCql(createTable);
    assertThat(executeCqlStatement(createTable)).isTrue();
  }

  /** Unsupported to read or write to UDT with inner UDT */
  @Test
  public void udtWithUdtField() {

    var testName = "udtWithUdtField";
    assertDropType(testName);
    assertDropType("address");
    assertDropTable(tableName(testName));

    assertCreateType(
        "address",
        """
        (
            street text,
            city text
        )
        """);

    assertCreateType(
        testName,
        """
        (
            name text,
            address frozen<address>
        )
        """);

    assertCreateTableForUdt(testName, "frozen<\"" + testName + "\">");

    assertTableCommand(keyspaceName, tableName(testName))
        .templated()
        .findOne("id", "no_rows", List.of())
        .hasSingleApiError(
            ProjectionException.Code.UNSUPPORTED_COLUMN_TYPES,
            ProjectionException.class,
            "The command included the following columns cannot be read: \"col_for_type_udtWithUdtField\"(UNSUPPORTED).");

    var insertDoc =
        """
        {
          "id": "1",
          "col_for_type_udtWithUdtField": {
            "name": "John Doe",
            "address": {
              "street": "123 Main St",
              "city": "Springfield"
            }
          }
        }
        """;
    assertTableCommand(keyspaceName, tableName(testName))
        .templated()
        .insertOne(insertDoc)
        .hasSingleApiError(
            DocumentException.Code.UNSUPPORTED_COLUMN_TYPES,
            DocumentException.class,
            "The command included the following columns that have unsupported data types: \"col_for_type_udtWithUdtField\"(UNSUPPORTED).");
  }

  /** Unsupported to read or write to UDT with inner list */
  @Test
  public void udtWithListField() {

    var testName = "udtWithListField";
    assertDropType(testName);
    assertDropTable(tableName(testName));

    assertCreateType(
        testName,
        """
        (
            colours frozen<list<text>>
        )
        """);

    assertCreateTableForUdt(testName, "\"" + testName + "\"");

    assertTableCommand(keyspaceName, tableName(testName))
        .templated()
        .findOne("id", "no_rows", List.of())
        .hasSingleApiError(
            ProjectionException.Code.UNSUPPORTED_COLUMN_TYPES,
            ProjectionException.class,
            "The command included the following columns cannot be read: \"col_for_type_udtWithListField\"(UNSUPPORTED).");

    var insertDoc =
        """
        {
          "id": "1",
          "col_for_type_udtWithListField": {
              "colours" : ["red"]
          }
        }
        """;
    assertTableCommand(keyspaceName, tableName(testName))
        .templated()
        .insertOne(insertDoc)
        .hasSingleApiError(
            DocumentException.Code.UNSUPPORTED_COLUMN_TYPES,
            DocumentException.class,
            "The command included the following columns that have unsupported data types: \"col_for_type_udtWithListField\"(UNSUPPORTED).");
  }

  /** Unsupported to read or write to UDT with inner set */
  @Test
  public void udtWithSetField() {

    var testName = "udtWithSetField";
    assertDropType(testName);
    assertDropTable(tableName(testName));

    assertCreateType(
        testName,
        """
        (
            colours frozen<set<text>>
        )
        """);

    assertCreateTableForUdt(testName, "\"" + testName + "\"");

    assertTableCommand(keyspaceName, tableName(testName))
        .templated()
        .findOne("id", "no_rows", List.of())
        .hasSingleApiError(
            ProjectionException.Code.UNSUPPORTED_COLUMN_TYPES,
            ProjectionException.class,
            "The command included the following columns cannot be read: \"col_for_type_udtWithSetField\"(UNSUPPORTED).");

    var insertDoc =
        """
        {
          "id": "1",
          "col_for_type_udtWithSetField": {
              "colours" : ["red"]
          }
        }
        """;
    assertTableCommand(keyspaceName, tableName(testName))
        .templated()
        .insertOne(insertDoc)
        .hasSingleApiError(
            DocumentException.Code.UNSUPPORTED_COLUMN_TYPES,
            DocumentException.class,
            "The command included the following columns that have unsupported data types: \"col_for_type_udtWithSetField\"(UNSUPPORTED).");
  }

  /** Unsupported to read or write to UDT with inner map */
  @Test
  public void udtWithMapField() {

    var testName = "udtWithMapField";
    assertDropType(testName);
    assertDropTable(tableName(testName));

    assertCreateType(
        testName,
        """
        (
            colours frozen<map<text, text>>
        )
        """);

    assertCreateTableForUdt(testName, "\"" + testName + "\"");

    assertTableCommand(keyspaceName, tableName(testName))
        .templated()
        .findOne("id", "no_rows", List.of())
        .hasSingleApiError(
            ProjectionException.Code.UNSUPPORTED_COLUMN_TYPES,
            ProjectionException.class,
            "The command included the following columns cannot be read: \"col_for_type_udtWithMapField\"(UNSUPPORTED).");

    var insertDoc =
        """
        {
          "id": "1",
          "col_for_type_udtWithMapField": {
              "colours" :{"red": "ff0000", "green": "00ff00", "blue": "0000ff"}
          }
        }
        """;
    assertTableCommand(keyspaceName, tableName(testName))
        .templated()
        .insertOne(insertDoc)
        .hasSingleApiError(
            DocumentException.Code.UNSUPPORTED_COLUMN_TYPES,
            DocumentException.class,
            "The command included the following columns that have unsupported data types: \"col_for_type_udtWithMapField\"(UNSUPPORTED).");
  }

  /** Unsupported to read or write to UDT with inner vector */
  @Test
  public void udtWithVectorField() {

    var testName = "udtWithVectorField";
    assertDropType(testName);
    assertDropTable(tableName(testName));

    assertCreateType(
        testName,
        """
        (
             name text,
             embed vector<FLOAT, 5>
        )
        """);

    assertCreateTableForUdt(testName, "\"" + testName + "\"");

    assertTableCommand(keyspaceName, tableName(testName))
        .templated()
        .findOne("id", "no_rows", List.of())
        .hasSingleApiError(
            ProjectionException.Code.UNSUPPORTED_COLUMN_TYPES,
            ProjectionException.class,
            "The command included the following columns cannot be read: \"col_for_type_udtWithVectorField\"(UNSUPPORTED).");

    var insertDoc =
        """
        {
          "id": "1",
          "col_for_type_udtWithVectorField": {
              "embed" : [1,2,3,4,5]
          }
        }
        """;
    assertTableCommand(keyspaceName, tableName(testName))
        .templated()
        .insertOne(insertDoc)
        .hasSingleApiError(
            DocumentException.Code.UNSUPPORTED_COLUMN_TYPES,
            DocumentException.class,
            "The command included the following columns that have unsupported data types: \"col_for_type_udtWithVectorField\"(UNSUPPORTED).");
  }
}
