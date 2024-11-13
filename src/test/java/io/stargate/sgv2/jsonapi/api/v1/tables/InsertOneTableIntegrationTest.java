package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistryTestData;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.util.Base64Util;
import io.stargate.sgv2.jsonapi.util.CqlVectorUtil;
import java.time.Instant;
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
  static final String TABLE_WITH_TEXT_COLUMNS = "insertOneTextColumnsTable";
  static final String TABLE_WITH_INT_COLUMNS = "insertOneIntColumnsTable";
  static final String TABLE_WITH_FP_COLUMNS = "insertOneFpColumnsTable";
  static final String TABLE_WITH_BINARY_COLUMN = "insertOneBinaryColumnsTable";
  static final String TABLE_WITH_DATETIME_COLUMNS = "insertOneDateTimeColumnsTable";
  static final String TABLE_WITH_UUID_COLUMN = "insertOneUuidColumnTable";
  static final String TABLE_WITH_INET_COLUMN = "insertOneInetColumnTable";
  static final String TABLE_WITH_LIST_COLUMNS = "insertOneListColumnsTable";
  static final String TABLE_WITH_SET_COLUMNS = "insertOneSetColumnsTable";
  static final String TABLE_WITH_MAP_COLUMNS = "insertOneMapColumnsTable";
  static final String TABLE_WITH_VECTOR_COLUMN = "insertOneVectorColumnTable";

  final JSONCodecRegistryTestData codecTestData = new JSONCodecRegistryTestData();

  @BeforeAll
  public final void createDefaultTables() {
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_TEXT_COLUMNS,
            Map.of(
                "idText", "text",
                "asciiText", "ascii",
                "varcharText", "text"),
            "idText")
        .wasSuccessful();

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_INT_COLUMNS,
            Map.of(
                "id", "text",
                "intValue", "int",
                "longValue", "bigint",
                "shortValue", "smallint",
                "byteValue", "tinyint",
                "bigIntegerValue", "varint"),
            "id")
        .wasSuccessful();

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_FP_COLUMNS,
            Map.of(
                "id", "text",
                "floatValue", "float",
                "doubleValue", "double",
                "decimalValue", "decimal"),
            "id")
        .wasSuccessful();

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(TABLE_WITH_BINARY_COLUMN, Map.of("id", "text", "binaryValue", "blob"), "id")
        .wasSuccessful();

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_DATETIME_COLUMNS,
            Map.of(
                "id", "text",
                "dateValue", "date",
                "durationValue", "duration",
                "timeValue", "time",
                "timestampValue", "timestamp"),
            "id")
        .wasSuccessful();

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_UUID_COLUMN,
            Map.of(
                "id", "text",
                "uuidValue", "uuid"),
            "id")
        .wasSuccessful();

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_INET_COLUMN,
            Map.of(
                "id", "text",
                "inetValue", "inet"),
            "id")
        .wasSuccessful();

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_LIST_COLUMNS,
            Map.of(
                "id",
                "text",
                "stringList",
                Map.of("type", "list", "valueType", "text"),
                "intList",
                Map.of("type", "list", "valueType", "int"),
                "doubleList",
                Map.of("type", "list", "valueType", "double")),
            "id")
        .wasSuccessful();

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_SET_COLUMNS,
            Map.of(
                "id",
                "text",
                "intSet",
                Map.of("type", "set", "valueType", "int"),
                "doubleSet",
                Map.of("type", "set", "valueType", "double"),
                "stringSet",
                Map.of("type", "set", "valueType", "text")),
            "id")
        .wasSuccessful();

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_MAP_COLUMNS,
            Map.of(
                "id",
                "text",
                "intMap",
                Map.of("type", "map", "keyType", "text", "valueType", "int"),
                "doubleMap",
                Map.of("type", "map", "keyType", "ascii", "valueType", "double"),
                "stringMap",
                Map.of("type", "map", "keyType", "text", "valueType", "text")),
            "id")
        .wasSuccessful();

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_VECTOR_COLUMN,
            Map.of(
                "id",
                "text",
                "vector",
                Map.of("type", "vector", "valueType", "float", "dimension", 3)),
            "id")
        .wasSuccessful();
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
      assertTableCommand(keyspaceName, TABLE_WITH_TEXT_COLUMNS)
          .templated()
          .insertOne(DOC_JSON)
          .wasSuccessful();

      // And verify that we can read it back
      assertTableCommand(keyspaceName, TABLE_WITH_TEXT_COLUMNS)
          .postFindOne("{ \"filter\": { \"idText\": \"abc\" } }")
          .wasSuccessful()
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

      assertTableCommand(keyspaceName, TABLE_WITH_TEXT_COLUMNS)
          .templated()
          .insertOne(DOC_JSON)
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
      assertTableCommand(keyspaceName, TABLE_WITH_INT_COLUMNS)
          .templated()
          .insertOne(intDoc("zero-fraction", "5.00"))
          .wasSuccessful();

      // and out comes 5
      assertTableCommand(keyspaceName, TABLE_WITH_INT_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"zero-fraction\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", intDoc("zero-fraction", "5"));
    }

    // [data-api#1429]: Test to verify that scientific is allowed for int types if  (and only if)
    // the fractional part is zero
    @Test
    void insertWithIntColumnsScientificNotation() {
      // In goes 1.23E+02
      assertTableCommand(keyspaceName, TABLE_WITH_INT_COLUMNS)
          .templated()
          .insertOne(intDoc("scientific-but-int", "1.23E+02"))
          .wasSuccessful();

      // and out comes 123
      assertTableCommand(keyspaceName, TABLE_WITH_INT_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"scientific-but-int\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", intDoc("scientific-but-int", "123"));
    }

    // [data-api#1429]: Test to verify that should there be real fraction, insert fails
    @Test
    void failWithNonZeroFractionPlain() {
      // Try with 12.5, should fail
      assertTableCommand(keyspaceName, TABLE_WITH_INT_COLUMNS)
          .templated()
          .insertOne(intDoc("non-zero-fraction", "12.5"))
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
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"fpRegular\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);
    }

    // [data-api#1428]: Test to verify Not-a-Number handling, NaN
    @Test
    void insertWithNaNOk() {
      // First check Float
      String docJSON = fpDoc("floatNan", "\"NaN\"", "0.25", "0.5");
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"floatNan\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);

      // Then double
      docJSON = fpDoc("doubleNan", "-2.5", "\"NaN\"", "0.5");
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"doubleNan\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);
    }

    // [data-api#1428]: Test to verify Not-a-Number handling, (positive) Infinity
    @Test
    void insertWithPositiveInfOk() {
      // First check Float
      String docJSON = fpDoc("floatInf", "\"Infinity\"", "0.25", "0.5");
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"floatInf\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);

      // Then double
      docJSON = fpDoc("doubleInf", "-2.5", "\"Infinity\"", "0.5");
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"doubleInf\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);
    }

    // [data-api#1428]: Test to verify Not-a-Number handling, Negative Infinity
    @Test
    void insertWithNegativeInfOk() {
      // First check Float
      String docJSON = fpDoc("floatNegInf", "\"-Infinity\"", "0.25", "0.5");
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"floatNegInf\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);

      // Then double
      docJSON = fpDoc("doubleNegInf", "-2.5", "\"-Infinity\"", "0.5");
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"doubleNegInf\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);
    }

    @Test
    void failWithUnrecognizedString() {
      // First float
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(fpDoc("floatUnknownString", "\"Bazillion\"", "1.0", "0.5"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              " value \"Bazillion\"",
              "Root cause: Unsupported String value: only");
      // Then double
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(fpDoc("doubleUnknownString", "\"Bazillion\"", "1.0", "0.5"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              " value \"Bazillion\"",
              "Root cause: Unsupported String value: only");

      // And finally BigDecimal: different error message because no String values accepted
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(fpDoc("decimalUnknownString", "0.5", "1.0", "\"Bazillion\""))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by the column data type can be included when inserting",
              "no codec matching value type",
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
    void insertValidBinaryValue() {
      final String docJSON =
          wrappedBinaryDoc("binarySimple", codecTestData.BASE64_PADDED_ENCODED_STR);
      assertTableCommand(keyspaceName, TABLE_WITH_BINARY_COLUMN)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();
      assertTableCommand(keyspaceName, TABLE_WITH_BINARY_COLUMN)
          .postFindOne("{ \"filter\": { \"id\": \"binarySimple\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);
    }

    @Test
    void failOnMalformedBase64() {
      assertTableCommand(keyspaceName, TABLE_WITH_BINARY_COLUMN)
          .templated()
          .insertOne(wrappedBinaryDoc("binaryBadBase64", "not-valid-base64!!!"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `BLOB` from");
    }

    @Test
    void failOnMalformedEJSONWrapper() {
      // Test with number first:
      assertTableCommand(keyspaceName, TABLE_WITH_BINARY_COLUMN)
          .templated()
          .insertOne(rawBinaryDoc("binaryFromNumber", "1234"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES, DocumentException.class, "binaryValue");
      // and then with String too; valid Base64, but not EJSON
      assertTableCommand(keyspaceName, TABLE_WITH_BINARY_COLUMN)
          .templated()
          .insertOne(
              rawBinaryDoc(
                  "binaryFromString", "\"" + codecTestData.BASE64_PADDED_ENCODED_STR + "\""))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES, DocumentException.class, "binaryValue");
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

  @Nested
  @Order(5)
  class InsertDatetimeColumns {
    @Test
    void insertValidDateTimeValues() {
      // NOTE: While `CqlDuration.from()` accepts both ISO-8601 "P"-notation (like "PT2H45M")
      //   and Cassandra's standard compact/readable notation (like "2h45m"),
      //   Output value will be ISO-8601 duration ("P"-notation)
      final String inputJSON =
          datetimeDoc(
              "datetimeValid", "2024-09-24", "2h45m", "12:45:01.005", "2024-09-24T14:06:59Z");
      final String outputJSON =
          datetimeDoc(
              "datetimeValid", "2024-09-24", "PT2H45M", "12:45:01.005", "2024-09-24T14:06:59Z");
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .templated()
          .insertOne(inputJSON)
          .wasSuccessful();
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"datetimeValid\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", outputJSON);
    }

    @Test
    void insertEJSONTimestampValue() {
      // NOTE: test for alternate input format -- EJSON-wrapper for Collection compatibility
      // -- for timestamp
      final long rawTimestamp = 1693036410000L;
      final String timestampISO8601 = Instant.ofEpochMilli(rawTimestamp).toString();
      final String inputJSON =
              """
          {
              "id": "datetimeValidAlt",
              "timestampValue": { "$date":  %d }
          }
          """
              .formatted(rawTimestamp);

      final String outputJSON =
              """
           {
              "id": "datetimeValidAlt",
              "timestampValue": "%s"
           }
           """
              .formatted(timestampISO8601);

      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .templated()
          .insertOne(inputJSON)
          .wasSuccessful();
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .postFindOne(
              """
          {
             "filter": { "id": "datetimeValidAlt" },
             "projection": {
                "id": 1,
                "timestampValue": 1
             }
          }
          """)
          .wasSuccessful()
          .hasJSONField("data.document", outputJSON);
    }

    @Test
    void insertValidNegativeDurationValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .templated()
          .insertOne(
                  """
                    {
                        "id": "%s",
                        "durationValue": "%s"
                    }
                    """
                  .formatted("datetimeNegDuration", "-8h10m"))
          .wasSuccessful();
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .postFindOne(
              """
                                  { "filter": { "id": "datetimeNegDuration" },
                                    "projection": { "durationValue": 1 }
                                  }
                              """)
          .wasSuccessful()
          .hasJSONField(
              "data.document",
                  """
                    { "durationValue": "%s" }
                    """
                  .formatted("-PT8H10M"));
    }

    @Test
    void failOnInvalidDateValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .templated()
          .insertOne(datetimeDoc("datetimeInvalidDate", "xxx", null, null, null))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `DATE` from",
              "Text 'xxx'");
    }

    @Test
    void failOnInvalidDurationValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .templated()
          .insertOne(datetimeDoc("datetimeInvalidDuration", null, "xxx", null, null))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `DURATION` from",
              "Unable to convert 'xxx'");
    }

    @Test
    void failOnInvalidTimeValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .templated()
          .insertOne(datetimeDoc("datetimeInvalidTime", null, null, "xxx", null))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `TIME` from",
              "Text 'xxx'");
    }

    @Test
    void failOnInvalidTimestampValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .templated()
          .insertOne(datetimeDoc("datetimeInvalidTimestamp", null, null, null, "xxx"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `TIMESTAMP` from",
              "Text 'xxx'");
    }

    private String datetimeDoc(
        String id,
        String dateValue,
        String durationValue,
        String timeValue,
        String timestampValue) {
      return
          """
                {
                    "id": "%s",
                    "dateValue": %s,
                    "durationValue": %s,
                    "timeValue": %s,
                    "timestampValue": %s
                }
                """
          .formatted(
              id, quote(dateValue), quote(durationValue), quote(timeValue), quote(timestampValue));
    }

    private String quote(String s) {
      if (s == null) {
        return "null";
      }
      return "\"" + s + "\"";
    }
  }

  @Nested
  @Order(6)
  class InsertUUIDColumns {
    @Test
    void insertValidUUIDValue() {
      final String docJSON = uuidDoc("uuidValid", "\"123e4567-e89b-12d3-a456-426614174000\"");
      assertTableCommand(keyspaceName, TABLE_WITH_UUID_COLUMN)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_UUID_COLUMN)
          .postFindOne("{ \"filter\": { \"id\": \"uuidValid\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);
    }

    @Test
    void failOnInvalidUUIDString() {
      assertTableCommand(keyspaceName, TABLE_WITH_UUID_COLUMN)
          .templated()
          .insertOne(uuidDoc("uuidInvalid", "\"xxx\""))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `UUID` from",
              "problem: Invalid UUID string: xxx");
    }

    // Test for non-String input
    @Test
    void failOnInvalidUUIDArray() {
      assertTableCommand(keyspaceName, TABLE_WITH_UUID_COLUMN)
          .templated()
          .insertOne(uuidDoc("uuidInvalid", "[1, 2, 3, 4]"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `UUID` from",
              "Root cause: no codec matching value type");
    }

    private String uuidDoc(String id, String uuidValueStr) {
      return
          """
                      {
                          "id": "%s",
                          "uuidValue": %s
                      }
                      """
          .formatted(id, uuidValueStr);
    }
  }

  @Nested
  @Order(7)
  class InsertInetColumn {
    @Test
    void insertValidInetValue() {
      final String docJSON = inetDoc("inetValid", "\"192.168.5.99\"");
      assertTableCommand(keyspaceName, TABLE_WITH_INET_COLUMN)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_INET_COLUMN)
          .postFindOne("{ \"filter\": { \"id\": \"inetValid\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);
    }

    @Test
    void failOnInvalidInetString() {
      assertTableCommand(keyspaceName, TABLE_WITH_INET_COLUMN)
          .templated()
          .insertOne(inetDoc("inetInvalid", "\"xxx\""))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `INET` from",
              "problem: Invalid IP address value 'xxx'");
    }

    // Test for non-String input
    @Test
    void failOnInvalidInetArray() {
      assertTableCommand(keyspaceName, TABLE_WITH_INET_COLUMN)
          .templated()
          .insertOne(inetDoc("inetInvalid", "[1, 2, 3, 4]"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `INET` from",
              "Root cause: no codec matching value type");
    }

    private String inetDoc(String id, String inetValueStr) {
      return
          """
                          {
                              "id": "%s",
                              "inetValue": %s
                          }
                          """
          .formatted(id, inetValueStr);
    }
  }

  @Nested
  @Order(8)
  class InsertListColumns {
    @Test
    void insertValidListValues() {
      // First with values for all fields (note: harder to use helper methods)
      String docJSON =
          """
                      { "id": "listValidFull",
                        "stringList": ["abc", "xyz"],
                        "intList": [1, 2, -42],
                        "doubleList": [0.0, -0.5, 3.125]
                      }
                      """;
      assertTableCommand(keyspaceName, TABLE_WITH_LIST_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_LIST_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"listValidFull\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);

      // And then just for int-list; null for string, missing double
      var insertDoc =
          """
                      { "id": "listValidPartial",
                        "stringList": null,
                        "intList": [3, -999, 42]
                      }
                      """;
      assertTableCommand(keyspaceName, TABLE_WITH_LIST_COLUMNS)
          .templated()
          .insertOne(insertDoc)
          .wasSuccessful();

      // If we ask for all (select * basically), get explicit empty Lists:
      assertTableCommand(keyspaceName, TABLE_WITH_LIST_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"listValidPartial\" } }")
          .wasSuccessful()
          .hasJSONField(
              "data.document",
              """
                      { "id": "listValidPartial",
                        "intList": [3, -999, 42]
                      }
                      """);

      // But if specifically just for intList, get just that
      // NOTE: id column(s) not auto-included unlike with Collections and "_id"
      assertTableCommand(keyspaceName, TABLE_WITH_LIST_COLUMNS)
          .postFindOne(
              """
                  { "filter": { "id": "listValidPartial" },
                    "projection": { "intList": 1 }
                  }
              """)
          .wasSuccessful()
          .hasJSONField(
              "data.document",
              """
                      {
                        "intList": [3, -999, 42]
                      }
                      """);
    }

    @Test
    void failOnNonArrayListValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_LIST_COLUMNS)
          .templated()
          .insertOne(
              """
      {
        "id":"listInvalid",
        "stringList":"abc"
      }
      """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `List(TEXT",
              "no codec matching value type");
    }

    @Test
    void failOnWrongListElementValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_LIST_COLUMNS)
          .templated()
          .insertOne(
              """
              {
                "id":"listInvalid",
                "intList":["abc"]
              }
              """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `INT`",
              "no codec matching (list/set) declared element type");
    }
  }

  @Nested
  @Order(9)
  class InsertSetColumns {
    @Test
    void insertValidSetValues() {
      // First with values for all fields (note: harder to use helper methods)
      String docJSON =
          """
                      { "id": "setValidFull",
                        "doubleSet": [0.0, -0.5, 3.125],
                        "intSet": [1, 2, -42],
                        "stringSet": ["abc", "xyz"]
                      }
                      """;
      assertTableCommand(keyspaceName, TABLE_WITH_SET_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_SET_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"setValidFull\" } }")
          .wasSuccessful()
          // also: ordering by data store is lexicographic, so differs from input order;
          // plus actual values are sorted as well
          .hasJSONField(
              "data.document",
              """
                      { "id": "setValidFull",
                        "doubleSet": [-0.5, 0.0, 3.125],
                        "intSet": [-42, 1, 2],
                        "stringSet": ["abc", "xyz"]
                      }
                      """);

      // And then just for int-list; null for string, missing double
      assertTableCommand(keyspaceName, TABLE_WITH_SET_COLUMNS)
          .templated()
          .insertOne(
              """
                      { "id": "setValidPartial",
                        "stringSet": null,
                        "intSet": [3, -999, 42]
                      }
                      """)
          .wasSuccessful();

      // If we ask for all (select * basically), get explicit empty Sets:
      assertTableCommand(keyspaceName, TABLE_WITH_SET_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"setValidPartial\" } }")
          .wasSuccessful()
          .hasJSONField(
              "data.document",
              """
                              { "id": "setValidPartial",
                                "intSet": [-999, 3, 42]
                              }
                              """);

      // But if specifically just for intSet, get just that
      // NOTE: id column(s) not auto-included unlike with Collections and "_id"
      assertTableCommand(keyspaceName, TABLE_WITH_SET_COLUMNS)
          .postFindOne(
              """
                                  { "filter": { "id": "setValidPartial" },
                                    "projection": { "intSet": 1 }
                                  }
                              """)
          .wasSuccessful()
          .hasJSONField(
              "data.document",
              """
                              {
                                "intSet": [-999, 3, 42]
                              }
                              """);
    }

    @Test
    void failOnNonArraySetValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_SET_COLUMNS)
          .templated()
          .insertOne(
              """
              {
                "id":"setInvalid",
                "intSet":"abc"
              }
              """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `Set(INT",
              "no codec matching value type");
    }

    @Test
    void failOnWrongSetElementValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_SET_COLUMNS)
          .templated()
          .insertOne(
              """
              {
                "id":"setInvalid",
                "doubleSet":["abc"]
              }
              """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `DOUBLE`",
              // Double is special since there are NaNs represented by Strings
              "Unsupported String value: only");
    }
  }

  @Nested
  @Order(10)
  class InsertMapColumns {
    @Test
    void insertValidMapValues() {
      // First with values for all fields (note: harder to use helper methods)
      String docJSON =
          """
                          { "id": "mapValidFull",
                            "doubleMap": {"a": 0.0,  "b":-0.5},
                            "intMap": {"i1": 1, "i2": 2, "i3": -42},
                            "stringMap": {"abc": "xyz"}
                          }
                          """;
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"mapValidFull\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);

      // And then just for int-Map; null for string, missing double
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(
              """
                              { "id": "mapValidPartial",
                                "stringMap": null,
                                "intMap": {"a": 3, "b": -999, "c": 42}
                              }
                              """)
          .wasSuccessful();

      // If we ask for all (select * basically), get explicit empty Maps:
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"mapValidPartial\" } }")
          .wasSuccessful()
          .hasJSONField(
              "data.document",
              """
                                      { "id": "mapValidPartial",
                                        "intMap": {"a": 3, "b": -999, "c": 42}
                                      }
                                      """);

      // But if specifically just for intMap, get just that
      // NOTE: id column(s) not auto-included unlike with Collections and "_id"
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .postFindOne(
              """
                                          { "filter": { "id": "mapValidPartial" },
                                            "projection": { "intMap": 1 }
                                          }
                                      """)
          .wasSuccessful()
          .hasJSONField(
              "data.document",
              """
                                      {
                                        "intMap": {"a": 3, "b": -999, "c": 42}
                                      }
                                      """);
    }

    @Test
    void failOnNonObjectForMap() {
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(
              """
                      {
                        "id":"mapInvalid",
                        "intMap":"abc"
                      }
                      """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `Map(TEXT => INT",
              "no codec matching value type");
    }

    @Test
    void failOnWrongMapValueType() {
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(
              """
                      {
                        "id":"mapInvalid",
                        "intMap":{"i1": "abc"}
                      }
                      """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `INT`",
              "actual value type `java.lang.String`");
    }
  }

  @Nested
  @Order(11)
  class InsertVectorColumns {
    @Test
    void insertValidVectorValueUsingList() {
      String docJSON =
          """
                      { "id": "vectorValid",
                        "vector": [0.0, -0.5, 3.125]
                      }
                      """;
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .postFindOne("{ \"filter\": { \"id\": \"vectorValid\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);
    }

    @Test
    void insertValidVectorValueUsingBase64() {
      final float[] floats = {1.0f, -0.5f, 2.5f};
      final byte[] floatsAsBytes = CqlVectorUtil.floatsToBytes(floats);

      String inputJSON =
              """
                          { "id": "vectorValidBase64",
                            "vector": {"$binary": "%s"}
                          }
                          """
              .formatted(Base64Util.encodeAsMimeBase64(floatsAsBytes));
      // Base64-encoded float array used in input but will be read back as Array:
      String expJSON =
          """
                          { "id": "vectorValidBase64",
                            "vector": [1.0, -0.5, 2.5]
                          }
                          """;
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .templated()
          .insertOne(inputJSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .postFindOne("{ \"filter\": { \"id\": \"vectorValidBase64\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", expJSON);
    }

    @Test
    void failOnNonArrayVectorValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .templated()
          .insertOne(
              """
              {
                "id": "vectorInvalid",
                "vector": "abc"
              }
              """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `Vector(FLOAT",
              "no codec matching value type");
    }

    @Test
    void failOnWrongVectorElementValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .templated()
          .insertOne(
              """
                      {
                        "id":" vectorInvalid",
                        "vector": ["abc", 123, false]
                      }
                      """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `Vector(FLOAT",
              "expected JSON Number value as Vector element at position #0");
    }

    @Test
    void failOnVectorSizeMismatch() {
      // Exp 3, but only gets 2:
      final byte[] floatsAsBytes = CqlVectorUtil.floatsToBytes(new float[] {1.0f, -0.5f});
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .templated()
          .insertOne(
                  """
                      { "id": "vectorValidBase64",
                        "vector": {"$binary": "%s"}
                      }
                      """
                  .formatted(Base64Util.encodeAsMimeBase64(floatsAsBytes)))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "Only values that are supported by",
              "Error trying to convert to targetCQLType `Vector(FLOAT",
              "expected vector of length 3, got one with 2 elements");
    }
  }
}
