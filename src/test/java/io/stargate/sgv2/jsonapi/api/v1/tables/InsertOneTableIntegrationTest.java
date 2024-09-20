package io.stargate.sgv2.jsonapi.api.v1.tables;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistryTestData;
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
  static final String TABLE_WITH_FP_COLUMNS = "findOneFpColumnsTable";
  static final String TABLE_WITH_BINARY_COLUMN = "findOneBinaryColumnsTable";

  final JSONCodecRegistryTestData codecTestData = new JSONCodecRegistryTestData();

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
    createTableWithColumns(
        TABLE_WITH_FP_COLUMNS,
        Map.of(
            "id",
            Map.of("type", "text"),
            "floatValue",
            Map.of("type", "float"),
            "doubleValue",
            Map.of("type", "double"),
            "decimalValue",
            Map.of("type", "decimal")),
        "id");
    createTableWithColumns(
        TABLE_WITH_BINARY_COLUMN,
        Map.of("id", Map.of("type", "text"), "binaryValue", Map.of("type", "blob")),
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
      insertOneInTable(TABLE_WITH_INT_COLUMNS, intDoc("zero-fraction", "5.00"));
      // and out comes 5
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_INT_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"zero-fraction\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", intDoc("zero-fraction", "5"));
    }

    // [data-api#1429]: Test to verify that scientific is allowed for int types if  (and only if)
    // the fractional part is zero
    @Test
    void insertWithIntColumnsScientificNotation() {
      // In goes 1.23E+02
      insertOneInTable(TABLE_WITH_INT_COLUMNS, intDoc("scientific-but-int", "1.23E+02"));
      // and out comes 123
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_INT_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"scientific-but-int\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", intDoc("scientific-but-int", "123"));
    }

    // [data-api#1429]: Test to verify that should there be real fraction, insert fails
    @Test
    void failWithNonZeroFractionPlain() {
      // Try with 12.5, should fail
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_INT_COLUMNS)
          .postInsertOne(intDoc("non-zero-fraction", "12.5"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Root cause: Rounding necessary");
    }

    private String intDoc(String id, String num) {
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

  @Nested
  @Order(3)
  class InsertFPColumns {
    @Test
    void insertWithPlainFPValues() {
      final String docJSON = fpDoc("fpRegular", "0.25", "-2.5", "0.75");
      insertOneInTable(TABLE_WITH_FP_COLUMNS, docJSON);
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"fpRegular\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", docJSON);
    }

    // [data-api#1428]: Test to verify Not-a-Number handling, NaN
    @Test
    void insertWithNaNOk() {
      // First check Float
      String docJSON = fpDoc("floatNan", "\"NaN\"", "0.25", "0.5");
      insertOneInTable(TABLE_WITH_FP_COLUMNS, docJSON);
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"floatNan\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", docJSON);

      // Then double
      docJSON = fpDoc("doubleNan", "-2.5", "\"NaN\"", "0.5");
      insertOneInTable(TABLE_WITH_FP_COLUMNS, docJSON);
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"doubleNan\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", docJSON);
    }

    // [data-api#1428]: Test to verify Not-a-Number handling, (positive) Infinity
    @Test
    void insertWithPositiveInfOk() {
      // First check Float
      String docJSON = fpDoc("floatInf", "\"Infinity\"", "0.25", "0.5");
      insertOneInTable(TABLE_WITH_FP_COLUMNS, docJSON);
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"floatInf\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", docJSON);

      // Then double
      docJSON = fpDoc("doubleInf", "-2.5", "\"Infinity\"", "0.5");
      insertOneInTable(TABLE_WITH_FP_COLUMNS, docJSON);
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"doubleInf\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", docJSON);
    }

    // [data-api#1428]: Test to verify Not-a-Number handling, Negative Infinity
    @Test
    void insertWithNegativeInfOk() {
      // First check Float
      String docJSON = fpDoc("floatNegInf", "\"-Infinity\"", "0.25", "0.5");
      insertOneInTable(TABLE_WITH_FP_COLUMNS, docJSON);
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"floatNegInf\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", docJSON);

      // Then double
      docJSON = fpDoc("doubleNegInf", "-2.5", "\"-Infinity\"", "0.5");
      insertOneInTable(TABLE_WITH_FP_COLUMNS, docJSON);
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"doubleNegInf\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", docJSON);
    }

    @Test
    void failWithUnrecognizedString() {
      // First float
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postInsertOne(fpDoc("floatUnknownString", "\"Bazillion\"", "1.0", "0.5"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              " value \"Bazillion\"",
              "Root cause: Unsupported String value: only");
      // Then double
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postInsertOne(fpDoc("doubleUnknownString", "\"Bazillion\"", "1.0", "0.5"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              " value \"Bazillion\"",
              "Root cause: Unsupported String value: only");

      // And finally BigDecimal: different error message because no String values accepted
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postInsertOne(fpDoc("decimalUnknownString", "0.5", "1.0", "\"Bazillion\""))
          .hasSingleApiError(
              DocumentException.Code.UNSUPPORTED_COLUMN_TYPES,
              DocumentException.class,
              "following columns that have unsupported data types",
              "\"decimalValue\"");
    }

    private String fpDoc(String id, String floatValue, String doubleValue, String bigDecValue) {
      return
          """
            {
                "id": "%s",
                "floatValue": %s,
                "doubleValue": %s,
                "decimalValue": %s
            }
            """
          .formatted(id, floatValue, doubleValue, bigDecValue);
    }
  }

  @Nested
  @Order(4)
  class InsertBinaryColumns {
    @Test
    void insertSimpleBinaryValue() {
      final String docJSON =
          wrappedBinaryDoc("binarySimple", codecTestData.BASE64_PADDED_ENCODED_STR);
      insertOneInTable(TABLE_WITH_BINARY_COLUMN, docJSON);
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_BINARY_COLUMN)
          .postFindOne("{ \"filter\": { \"id\": \"binarySimple\" } }")
          .hasNoErrors()
          .hasJSONField("data.document", docJSON);
    }

    @Test
    void failOnMalformedBase64() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_BINARY_COLUMN)
          .postInsertOne(wrappedBinaryDoc("binaryBadBase64", "not-valid-base64!!!"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `BLOB` from");
    }

    @Test
    void failOnMalformedEJSONWrapper() {
      // Test with number first:
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_BINARY_COLUMN)
          .postInsertOne(rawBinaryDoc("binaryFromNumber", "1234"))
          .hasSingleApiError(
              // Not optimal: to be improved in future:
              DocumentException.Code.UNSUPPORTED_COLUMN_TYPES, DocumentException.class);
      // and then with String too; valid Base64, but not EJSON
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_BINARY_COLUMN)
          .postInsertOne(
              rawBinaryDoc(
                  "binaryFromString", "\"" + codecTestData.BASE64_PADDED_ENCODED_STR + "\""))
          .hasSingleApiError(
              // Not optimal: to be improved in future:
              DocumentException.Code.UNSUPPORTED_COLUMN_TYPES, DocumentException.class);
    }

    private String wrappedBinaryDoc(String id, String base64Binary) {
      return rawBinaryDoc(id, "{\"$binary\": \"%s\"}".formatted(base64Binary));
    }

    private String rawBinaryDoc(String id, String rawBinaryValue) {
      return
          """
                {
                    "id": "%s",
                    "binaryValue": %s
                }
                """
          .formatted(id, rawBinaryValue);
    }
  }
}
