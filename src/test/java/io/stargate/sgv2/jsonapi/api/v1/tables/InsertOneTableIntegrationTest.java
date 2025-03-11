package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistryTestData;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.util.Base64Util;
import io.stargate.sgv2.jsonapi.util.CqlVectorUtil;
import java.time.Instant;
import java.util.List;
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
  static final String TABLE_WITH_VECTOR_KEY = "insertOneVectorKeyTable";

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
            Map.ofEntries(
                Map.entry("id", "text"),
                // **************** map can be inserted in object/tuple format ****************
                Map.entry("textMap", Map.of("type", "map", "keyType", "text", "valueType", "text")),
                Map.entry(
                    "asciiMap", Map.of("type", "map", "keyType", "ascii", "valueType", "ascii")),
                Map.entry("inetMap", Map.of("type", "map", "keyType", "inet", "valueType", "inet")),
                Map.entry("dateMap", Map.of("type", "map", "keyType", "date", "valueType", "date")),
                Map.entry("timeMap", Map.of("type", "map", "keyType", "time", "valueType", "time")),
                Map.entry(
                    "timestampMap",
                    Map.of("type", "map", "keyType", "timestamp", "valueType", "timestamp")),
                Map.entry("uuidMap", Map.of("type", "map", "keyType", "uuid", "valueType", "uuid")),
                // timeUuid as key/value is excluded, since we deliberately restrain timeUuid on API
                // side, may change in the future.
                // counter as key/value is excluded, since counters are not allowed within
                // collections in cql.
                // duration as key is excluded, since duration types are not supported within
                // non-frozen map keys, and API does not support frozen.
                Map.entry(
                    "durationMap",
                    Map.of("type", "map", "keyType", "text", "valueType", "duration")),
                // **************** map must be inserted as tuple format ****************
                Map.entry("intMap", Map.of("type", "map", "keyType", "int", "valueType", "int")),
                Map.entry(
                    "tinyintMap",
                    Map.of("type", "map", "keyType", "tinyint", "valueType", "tinyint")),
                Map.entry(
                    "varintMap", Map.of("type", "map", "keyType", "varint", "valueType", "varint")),
                Map.entry(
                    "floatMap", Map.of("type", "map", "keyType", "float", "valueType", "float")),
                Map.entry(
                    "bigintMap", Map.of("type", "map", "keyType", "bigint", "valueType", "bigint")),
                Map.entry(
                    "smallintMap",
                    Map.of("type", "map", "keyType", "smallint", "valueType", "smallint")),
                Map.entry(
                    "decimalMap",
                    Map.of("type", "map", "keyType", "decimal", "valueType", "decimal")),
                Map.entry(
                    "doubleMap", Map.of("type", "map", "keyType", "double", "valueType", "double")),
                Map.entry(
                    "booleanMap",
                    Map.of("type", "map", "keyType", "boolean", "valueType", "boolean")),
                Map.entry(
                    "blobMap", Map.of("type", "map", "keyType", "blob", "valueType", "blob"))),
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

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_VECTOR_KEY,
            Map.of(
                "vectorId",
                Map.of("type", "vector", "valueType", "float", "dimension", 3),
                "textValue",
                "text"),
            "vectorId")
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
          .wasSuccessful()
          .hasInsertedIds(List.of("abc"));

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
              "\"asciiText\"(ascii) - Cause: String contains non-ASCII character at index #12");
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
          .wasSuccessful()
          .hasInsertedIds(List.of("zero-fraction"));

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
          .wasSuccessful()
          .hasInsertedIds(List.of("scientific-but-int"));

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
              "Cause: Rounding necessary");
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
          .wasSuccessful()
          .hasInsertedIds(List.of("fpRegular"));

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
          .wasSuccessful()
          .hasInsertedIds(List.of("floatNan"));

      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"floatNan\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);

      // Then double
      docJSON = fpDoc("doubleNan", "-2.5", "\"NaN\"", "0.5");
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful()
          .hasInsertedIds(List.of("doubleNan"));

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
          .wasSuccessful()
          .hasInsertedIds(List.of("floatInf"));

      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"floatInf\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);

      // Then double
      docJSON = fpDoc("doubleInf", "-2.5", "\"Infinity\"", "0.5");
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful()
          .hasInsertedIds(List.of("doubleInf"));

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
          .wasSuccessful()
          .hasInsertedIds(List.of("floatNegInf"));

      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"floatNegInf\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);

      // Then double
      docJSON = fpDoc("doubleNegInf", "-2.5", "\"-Infinity\"", "0.5");
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful()
          .hasInsertedIds(List.of("doubleNegInf"));
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
              "\"floatValue\"(float) - Cause: Unsupported String value: only \"NaN\", \"Infinity\" and \"-Infinity\" supported");
      // Then double
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(fpDoc("doubleUnknownString", "0.5", "\"Bazillion\"", "0.5"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "\"doubleValue\"(float) - Cause: Unsupported String value: only \"NaN\", \"Infinity\" and \"-Infinity\" supported");

      // And finally BigDecimal: different error message because no String values accepted
      assertTableCommand(keyspaceName, TABLE_WITH_FP_COLUMNS)
          .templated()
          .insertOne(fpDoc("decimalUnknownString", "0.5", "1.0", "\"Bazillion\""))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "\"decimalValue\"(float) - Cause: Unsupported String value: only \"NaN\", \"Infinity\" and \"-Infinity\" supported");
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
          .wasSuccessful()
          .hasInsertedIds(List.of("binarySimple"));
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
              "\"binaryValue\"(blob) - Cause: Unsupported JSON value in EJSON $binary wrapper: String not valid Base64-encoded content, problem: Illegal character '-' (code 0x2d) in base64 content");
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
          .wasSuccessful()
          .hasInsertedIds(List.of("datetimeValid"));
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
          .wasSuccessful()
          .hasInsertedIds(List.of("datetimeValidAlt"));
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
          .wasSuccessful()
          .hasInsertedIds(List.of("datetimeNegDuration"));
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
              "\"dateValue\"(date) - Cause: Invalid String value for type `DATE`; problem: Text 'xxx' could not be parsed at index 0");
    }

    @Test
    void failOnInvalidDurationValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .templated()
          .insertOne(datetimeDoc("datetimeInvalidDuration", null, "xxx", null, null))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "\"durationValue\"(duration) - Cause: Invalid String value for type `DURATION`; problem: Unable to convert 'xxx' to a duration");
    }

    @Test
    void failOnInvalidTimeValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .templated()
          .insertOne(datetimeDoc("datetimeInvalidTime", null, null, "xxx", null))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "\"timeValue\"(time) - Cause: Invalid String value for type `TIME`; problem: Text 'xxx' could not be parsed at index 0");
    }

    @Test
    void failOnInvalidTimestampValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_DATETIME_COLUMNS)
          .templated()
          .insertOne(datetimeDoc("datetimeInvalidTimestamp", null, null, null, "xxx"))
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "\"timestampValue\"(timestamp) - Cause: Invalid String value for type `TIMESTAMP`; problem: Text 'xxx' could not be parsed at index 0");
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
          .wasSuccessful()
          .hasInsertedIds(List.of("uuidValid"));

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
              "\"uuidValue\"(uuid) - Cause: Invalid String value for type `UUID`; problem: Invalid UUID string: xxx");
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
              "\"uuidValue\"(uuid) - Cause: no codec matching value type");
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
          .wasSuccessful()
          .hasInsertedIds(List.of("inetValid"));

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
              "\"inetValue\"(inet) - Cause: Invalid String value for type `INET`; problem: Invalid IP address value 'xxx'");
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
              "\"inetValue\"(inet) - Cause: no codec matching value type");
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
          .wasSuccessful()
          .hasInsertedIds(List.of("listValidFull"));

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
          .wasSuccessful()
          .hasInsertedIds(List.of("listValidPartial"));

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
              "\"stringList\"(list) - Cause: no codec matching value type");
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
              "\"intList\"(list) - Cause: no codec matching (list/set) declared element type `INT`, actual value type `java.lang.String`");
    }

    @Test
    void failOnInsertNullValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_LIST_COLUMNS)
          .templated()
          .insertOne(
              """
                      {
                        "id":"listInvalid",
                        "intList":[null]
                      }
                      """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "null values are not allowed in list column");
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
          .wasSuccessful()
          .hasInsertedIds(List.of("setValidFull"));

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
          .wasSuccessful()
          .hasInsertedIds(List.of("setValidPartial"));

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
              "\"intSet\"(set) - Cause: no codec matching value type");
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
              "\"doubleSet\"(set) - Cause: Unsupported String value: only \"NaN\", \"Infinity\" and \"-Infinity\" supported");
    }

    @Test
    void failOnInsertNullValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_SET_COLUMNS)
          .templated()
          .insertOne(
              """
                      {
                        "id":"setInvalid",
                        "doubleSet":[null]
                      }
                      """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "null values are not allowed in set column");
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
                  "textMap": {"key1": "value1"},
                  "asciiMap": {"key1": "value1"},
                  "inetMap": {"192.168.1.1": "192.168.1.2"},
                  "dateMap": {"2024-09-24": "2024-09-25"},
                  "timeMap": {"12:00:01": "13:00:01"},
                  "timestampMap": {"2024-09-24T14:06:59Z": "2024-09-25T14:06:59Z"},
                  "uuidMap": {"123e4567-e89b-12d3-a456-426614174000": "123e4567-e89b-12d3-a456-426614174001"},
                  "durationMap": {"key1": "PT2H45M"},
                  "intMap": [[1, 2]],
                  "tinyintMap": [[1, 2]],
                  "varintMap": [[1, 2]],
                  "floatMap": [[1.0, 2.0]],
                  "bigintMap": [[1, 2]],
                  "smallintMap": [[1, 2]],
                  "decimalMap": [[1.5, 2.75]],
                  "doubleMap": [[1.0, 2.0]],
                  "booleanMap": [[true, false]],
                  "blobMap": [
                    [
                      {
                        "$binary": "SGVsbG8gV29ybGQ="
                      },
                      {
                        "$binary": "SGVsbG8gV29ybGQ="
                      }
                    ]
                  ]
                }
              """;
      // Only TEXT/ASCII key map are returned as object format
      // Other key type maps are returned as tuple format
      String expectedJson =
          """
                    { "id": "mapValidFull",
                      "textMap": {"key1": "value1"},
                      "asciiMap": {"key1": "value1"},
                      "inetMap": [[ "192.168.1.1", "192.168.1.2"]],
                      "dateMap": [["2024-09-24", "2024-09-25"]],
                      "timeMap": [["12:00:01", "13:00:01"]],
                      "timestampMap": [["2024-09-24T14:06:59Z", "2024-09-25T14:06:59Z"]],
                      "uuidMap": [["123e4567-e89b-12d3-a456-426614174000", "123e4567-e89b-12d3-a456-426614174001"]],
                      "durationMap": {"key1": "PT2H45M"},
                      "intMap": [[1, 2]],
                      "tinyintMap": [[1, 2]],
                      "varintMap": [[1, 2]],
                      "floatMap": [[1.0, 2.0]],
                      "bigintMap": [[1, 2]],
                      "smallintMap": [[1, 2]],
                      "decimalMap": [[1.5, 2.75]],
                      "doubleMap": [[1.0, 2.0]],
                      "booleanMap": [[true, false]],
                      "blobMap": [
                        [
                          {
                            "$binary": "SGVsbG8gV29ybGQ="
                          },
                          {
                            "$binary": "SGVsbG8gV29ybGQ="
                          }
                        ]
                      ]
                    }
                  """;
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful()
          .hasInsertedIds(List.of("mapValidFull"));

      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"mapValidFull\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", expectedJson);

      // And then test some null values
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(
              """
                              { "id": "mapValidPartial",
                                "asciiMap": null,
                                "booleanMap": null,
                                "textMap": {"a": "b"}
                              }
                              """)
          .wasSuccessful()
          .hasInsertedIds(List.of("mapValidPartial"));

      // If we ask for all (select * basically), get explicit empty Maps:
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"mapValidPartial\" } }")
          .wasSuccessful()
          .hasJSONField(
              "data.document",
              """
                                      { "id": "mapValidPartial",
                                        "textMap": {"a": "b"}
                                      }
                                      """);

      // But if specifically just for textMap, get just that
      // NOTE: id column(s) not auto-included unlike with Collections and "_id"
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .postFindOne(
              """
                                          { "filter": { "id": "mapValidPartial" },
                                            "projection": { "textMap": 1 }
                                          }
                                      """)
          .wasSuccessful()
          .hasJSONField(
              "data.document",
              """
                        {
                          "textMap": {"a": "b"}
                        }
                        """);

      // Project a non-string key map column
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .postFindOne(
              """
                            { "filter": { "id": "mapValidFull" },
                              "projection": { "blobMap": 1 }
                            }
                        """)
          .wasSuccessful()
          .hasJSONField(
              "data.document",
              """
                        {
                            "blobMap": [
                                [
                                      {
                                        "$binary": "SGVsbG8gV29ybGQ="
                                      },
                                      {
                                        "$binary": "SGVsbG8gV29ybGQ="
                                      }
                                ]
                            ]
                        }
                        """);
    }

    @Test
    void insertMapAllTupleFormat() {
      String docJSON =
          """
                    { "id": "mapValidFullTuple",
                      "textMap": [["key1", "value1"]],
                      "asciiMap": [["key1", "value1"]],
                      "inetMap": [["192.168.1.1", "192.168.1.2"]],
                      "dateMap": [["2024-09-24", "2024-09-25"]],
                      "timeMap": [["12:00:01", "13:00:01"]],
                      "timestampMap": [["2024-09-24T14:06:59Z", "2024-09-25T14:06:59Z"]],
                      "uuidMap": [["123e4567-e89b-12d3-a456-426614174000", "123e4567-e89b-12d3-a456-426614174001"]],
                      "durationMap": [["key1", "PT2H45M"]],
                      "intMap": [[1, 2]],
                      "tinyintMap": [[1, 2]],
                      "varintMap": [[1, 2]],
                      "floatMap": [[1.0, 2.0]],
                      "bigintMap": [[1, 2]],
                      "smallintMap": [[1, 2]],
                      "decimalMap": [[1.5, 2.75]],
                      "doubleMap": [[1.0, 2.0]],
                      "booleanMap": [[true, false]],
                      "blobMap": [
                        [
                          {
                            "$binary": "SGVsbG8gV29ybGQ="
                          },
                          {
                            "$binary": "SGVsbG8gV29ybGQ="
                          }
                        ]
                      ]
                    }
                  """;
      // Only TEXT/ASCII key map are returned as object format
      // Other key type maps are returned as tuple format
      String expectedJson =
          """
                        { "id": "mapValidFullTuple",
                          "textMap": {"key1": "value1"},
                          "asciiMap": {"key1": "value1"},
                          "inetMap": [[ "192.168.1.1", "192.168.1.2"]],
                          "dateMap": [["2024-09-24", "2024-09-25"]],
                          "timeMap": [["12:00:01", "13:00:01"]],
                          "timestampMap": [["2024-09-24T14:06:59Z", "2024-09-25T14:06:59Z"]],
                          "uuidMap": [["123e4567-e89b-12d3-a456-426614174000", "123e4567-e89b-12d3-a456-426614174001"]],
                          "durationMap": {"key1": "PT2H45M"},
                          "intMap": [[1, 2]],
                          "tinyintMap": [[1, 2]],
                          "varintMap": [[1, 2]],
                          "floatMap": [[1.0, 2.0]],
                          "bigintMap": [[1, 2]],
                          "smallintMap": [[1, 2]],
                          "decimalMap": [[1.5, 2.75]],
                          "doubleMap": [[1.0, 2.0]],
                          "booleanMap": [[true, false]],
                          "blobMap": [
                            [
                              {
                                "$binary": "SGVsbG8gV29ybGQ="
                              },
                              {
                                "$binary": "SGVsbG8gV29ybGQ="
                              }
                            ]
                          ]
                        }
                      """;
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful()
          .hasInsertedIds(List.of("mapValidFullTuple"));

      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": \"mapValidFullTuple\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", expectedJson);
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
              "\"intMap\"(map) - Cause: no codec matching value type");
    }

    @Test
    void failOnWrongMapValueType() {
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(
              """
                      {
                        "id":"mapInvalid",
                        "intMap":[[1, "abc"]]
                      }
                      """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "\"intMap\"(map) - Cause: no codec matching map declared value type `INT`, actual type `java.lang.String`");
    }

    @Test
    void failOnWrongMapKeyType() {
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(
              """
                              {
                                "id":"mapInvalid",
                                "intMap":[["abc",1]]
                              }
                              """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "\"intMap\"(map) - Cause: no codec matching map declared key type `INT`, actual type `java.lang.String`");
    }

    @Test
    void failOnInsertNullKey() {
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(
              """
                      {
                        "id":"mapInvalid",
                        "intMap":[[null,1]]
                      }
                      """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "null keys/values are not allowed in map column");
    }

    @Test
    void failOnInsertNullValue() {
      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(
              """
                      {
                        "id":"mapInvalid",
                        "intMap":[[1, null]]
                      }
                      """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "null keys/values are not allowed in map column");

      assertTableCommand(keyspaceName, TABLE_WITH_MAP_COLUMNS)
          .templated()
          .insertOne(
              """
                      {
                        "id":"mapInvalid",
                        "textMap":{"abc": null}
                      }
                      """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "null keys/values are not allowed in map column");
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
          .wasSuccessful()
          .hasInsertedIds(List.of("vectorValid"));

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
          .wasSuccessful()
          .hasInsertedIds(List.of("vectorValidBase64"));

      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .postFindOne("{ \"filter\": { \"id\": \"vectorValidBase64\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", expJSON);
    }

    // [databind#1817]
    @Test
    void insertValidNullVectorValue() {
      // To read vector back as null can either omit or add null; issue reported with
      // former so test that first
      String docJSON1 = "{\"id\": \"vectorNULL\"}";
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .templated()
          .insertOne(docJSON1)
          .wasSuccessful()
          .hasInsertedIds(List.of("vectorNULL"));
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .postFindOne("{ \"filter\": { \"id\": \"vectorNULL\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON1);

      // But then let's try with explicit null as well
      String docJSON2 = "{\"id\": \"vectorNULL2\", \"vector\": null}";
      // Returned version has no `vector` field, even when passing explicit null
      String expJSON2 = "{\"id\": \"vectorNULL2\"}";
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .templated()
          .insertOne(docJSON2)
          .wasSuccessful()
          .hasInsertedIds(List.of("vectorNULL2"));
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .postFindOne("{ \"filter\": { \"id\": \"vectorNULL2\" } }")
          .wasSuccessful()
          .hasJSONField("data.document", expJSON2);
    }

    @Test
    void insertValidVectorKey() {
      String docJSON =
          """
                          {
                            "vectorId": [0.75, -0.25, 0.5],
                            "textValue": "stuff"
                          }
                          """;
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_KEY)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful()
          .hasInsertedIds(List.of(List.of(0.75f, -0.25f, 0.5f)));

      // NOTE: cannot filter by Vector (yet?) so just verify without one knowing
      // we inserted just one row
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_KEY)
          .postFindOne("{ }")
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);
    }

    @Test
    void failOnNonArrayVectorValue() {
      // if we use a string, the server will thing we are trying to do vectorize
      assertTableCommand(keyspaceName, TABLE_WITH_VECTOR_COLUMN)
          .templated()
          .insertOne(
              """
              {
                "id": "vectorInvalid",
                "vector": 1
              }
              """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "vector(vector) - Cause: no codec matching value type");
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
              "vector(vector) - Cause: expected JSON Number value as Vector element at position #0 (of 3), instead have");
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
              "vector(vector) - Cause: expected vector of length 3, got one with 2 elements");
    }

    @Test
    void insertDifferentVectorizeDimensions() {
      // Two vector columns with same provider and model, but different dimension, is now allowed
      final String tableName = "insertOneMultipleVectorizeDiffDimensionsTable";

      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              tableName,
              Map.of(
                  "id",
                  "text",
                  "vector1",
                  Map.of(
                      "type",
                      "vector",
                      "dimension",
                      "5",
                      "service",
                      Map.of("provider", "openai", "modelName", "text-embedding-3-small")),
                  "vector2",
                  Map.of(
                      "type",
                      "vector",
                      "dimension",
                      "10",
                      "service",
                      Map.of("provider", "openai", "modelName", "text-embedding-3-small"))),
              "id")
          .wasSuccessful();

      // will fail the token
      assertTableCommand(keyspaceName, tableName)
          .templated()
          .insertOne(
              """
                                  { "id": "1",
                                    "vector1": "1234",
                                    "vector2": "5678"
                                  }
                                  """)
          .hasSingleApiError(
              ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR.name(),
              "The Embedding Provider returned a HTTP client error: Provider: openai; HTTP Status: 401; Error Message: Incorrect API key provided: test_emb");
    }

    @Test
    void insertDifferentVectorizeModels() {
      // Two vector columns with same provider, different models, different dimensions
      // is supported
      String tableName = "insertOneMultipleVectorizeDiffModelsTable";

      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              tableName,
              Map.of(
                  "id",
                  "text",
                  "vector1",
                  Map.of(
                      "type",
                      "vector",
                      "dimension",
                      "5",
                      "service",
                      Map.of("provider", "openai", "modelName", "text-embedding-3-small")),
                  "vector2",
                  Map.of(
                      "type",
                      "vector",
                      "dimension",
                      "256",
                      "service",
                      Map.of("provider", "openai", "modelName", "text-embedding-3-large"))),
              "id")
          .wasSuccessful();

      // wont have the correct token
      assertTableCommand(keyspaceName, tableName)
          .templated()
          .insertOne(
              """
                                    { "id": "1",
                                        "vector1": "1234",
                                        "vector2": "5678"
                                    }
                                    """)
          .hasSingleApiError(
              ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR.name(),
              "The Embedding Provider returned a HTTP client error: Provider: openai; HTTP Status: 401; Error Message: Incorrect API key provided: test_emb");
    }

    @Test
    void insertDifferentVectorizeProviders() {
      // Two columns with different providers, not allowed for now
      String tableName = "insertOneMultipleVectorizeDiffProvidersTable";

      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              tableName,
              Map.of(
                  "id",
                  "text",
                  "vector1",
                  Map.of(
                      "type",
                      "vector",
                      "dimension",
                      "5",
                      "service",
                      Map.of("provider", "openai", "modelName", "text-embedding-3-small")),
                  "vector2",
                  Map.of(
                      "type",
                      "vector",
                      "dimension",
                      "768",
                      "service",
                      Map.of("provider", "jinaAI", "modelName", "jina-embeddings-v2-base-en"))),
              "id")
          .wasSuccessful();

      // this will error that the embedding provider key was not found
      assertTableCommand(keyspaceName, tableName)
          .templated()
          .insertOne(
              """
                                  { "id": "1",
                                    "vector1": "1234",
                                    "vector2": "5678"
                                  }
                                  """)
          .hasSingleApiError(
              ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR.name(),
              "Provider: openai; HTTP Status: 401; Error Message: Incorrect API key provided: test_emb");
    }
  }
}
