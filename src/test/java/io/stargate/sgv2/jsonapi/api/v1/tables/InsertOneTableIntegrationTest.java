package io.stargate.sgv2.jsonapi.api.v1.tables;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class InsertOneTableIntegrationTest extends AbstractTableIntegrationTestBase {
  static final String TABLE_WITH_TEXT_COLUMNS = "findOneTextColumnsTable";
  static final String TABLE_WITH_INT_COLUMNS = "findOneIntColumnsTable";

  @BeforeAll
  public final void createDefaultTables() {
    createTableWithColumns(
        TABLE_WITH_TEXT_COLUMNS,
        Map.of(
            "idText",
            Map.of("type", "text"),
            "asciiText",
            Map.of("type", "ascii"),
            "varcharText",
            Map.of("type", "text")),
        "idText");
    createTableWithColumns(
        TABLE_WITH_INT_COLUMNS,
        Map.of(
            "id",
            Map.of("type", "text"),
            "intValue",
            Map.of("type", "int"),
            "longValue",
            Map.of("type", "bigint"),
            "shortValue",
            Map.of("type", "smallint"),
            "byteValue",
            Map.of("type", "tinyint"),
            "bigIntegerValue",
            Map.of("type", "varint")),
        "id");
  }

  @Nested
  @Order(1)
  class InsertTextColumns {
    public final String STRING_UTF8_WITH_2BYTE_CHAR = "utf8-2-byte-\u00a2"; // cent symbol
    public final String STRING_UTF8_WITH_3BYTE_CHAR = "utf8-3-byte-\u20ac"; // euro symbol

    @Test
    void insertWithTextColumns() {
      final String DOC_JSON =
              """
                                        {
                                            "idText": "abc",
                                            "asciiText": "safe value",
                                            "varcharText": "%s/%s"
                                        }
                                        """
              .formatted(STRING_UTF8_WITH_2BYTE_CHAR, STRING_UTF8_WITH_3BYTE_CHAR);
      insertOneInTable(TABLE_WITH_TEXT_COLUMNS, DOC_JSON);

      // And verify that we can read it back
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_TEXT_COLUMNS)
          .postFindOne("{ \"filter\": { \"idText\": \"abc\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", DOC_JSON);
    }

    @Test
    void failTryingToInsertNonAscii() {
      final String DOC_JSON =
              """
                      {
                          "idText": "def",
                          "asciiText": "%s",
                          "varcharText": "safe value"
                      }
                      """
              .formatted(STRING_UTF8_WITH_2BYTE_CHAR);
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_TEXT_COLUMNS)
          .postInsertOne(DOC_JSON)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "String contains non-ASCII character");
    }
  }

  @Nested
  @Order(2)
  class InsertIntColumns {
    // [data-api#1429]: Test to verify that all-zero fractional parts are ok for int types
    @Test
    void insertWithIntColumnsZeroFractional() {
      // In goes 5.00
      insertOneInTable(TABLE_WITH_INT_COLUMNS, numDoc("zero-fraction", "5.00"));
      // and out comes 5
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_INT_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"zero-fraction\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", numDoc("zero-fraction", "5"));
    }

    // [data-api#1429]: Test to verify that scientific is allowed for int types if  (and only if)
    // the fractional part is zero
    @Test
    void insertWithIntColumnsScientificNotation() {
      // In goes 1.23E+02
      insertOneInTable(TABLE_WITH_INT_COLUMNS, numDoc("scientific-but-int", "1.23E+02"));
      // and out comes 123
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_INT_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"scientific-but-int\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", numDoc("scientific-but-int", "123"));
    }

    // [data-api#1429]: Test to verify that should there be real fraction, insert fails
    @Test
    void failWithNonZeroFractionPlain() {
      // Try with 12.5, should fail
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_INT_COLUMNS)
          .postInsertOne(numDoc("non-zero-fraction", "12.5"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Root cause: Rounding necessary");
    }

    private String numDoc(String id, String num) {
      return
          """
                                            {
                                                "id": "%s",
                                                "byteValue": %s,
                                                "shortValue": %s,
                                                "intValue": %s,
                                                "longValue": %s,
                                                "bigIntegerValue": %s
                                            }
                                            """
          .formatted(id, num, num, num, num, num);
    }
  }
}
