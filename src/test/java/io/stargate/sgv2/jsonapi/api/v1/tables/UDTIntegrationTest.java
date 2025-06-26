package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class UDTIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final String TYPE_NAME = "address";

  private static final Map<String, Object> TYPE_DEF = Map.of("city", "text", "postcode", "text");

  private static final String TABLE = "udt_table";

  @BeforeAll
  public static void createTypeAndTable() {
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createType(TYPE_NAME, TYPE_DEF)
        .wasSuccessful();

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE,
            Map.ofEntries(
                Map.entry("id", "text"),
                Map.entry("address", Map.of("type", "userDefined", "udtName", TYPE_NAME)),
                Map.entry(
                    "listOfAddress",
                    Map.of(
                        "type",
                        "list",
                        "valueType",
                        Map.of("type", "userDefined", "udtName", TYPE_NAME))),
                Map.entry(
                    "setOfAddress",
                    Map.of(
                        "type",
                        "set",
                        "valueType",
                        Map.of("type", "userDefined", "udtName", TYPE_NAME))),
                Map.entry(
                    "mapOfAddress",
                    Map.of(
                        "type",
                        "map",
                        "keyType",
                        "text",
                        "valueType",
                        Map.of("type", "userDefined", "udtName", TYPE_NAME)))),
            "id")
        .wasSuccessful();
  }

  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class CreateType {

    private static Stream<Arguments> unsupportedUDTFields() {
      return Stream.of(
          // list/set/map as field are not supported
          Arguments.of(Map.of("listField", Map.of("type", "list", "valueType", "text"))),
          // nested UDT as field is not supported
          Arguments.of(Map.of("address", Map.of("type", "userDefined", "udtName", "address"))));
    }

    @ParameterizedTest
    @MethodSource("unsupportedUDTFields")
    public void unsupportedUDTField(Map<String, Object> fields) {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createType("type_with_supported_filed", fields)
          .hasSingleApiError(
              SchemaException.Code.UNSUPPORTED_TYPE_FIELD,
              SchemaException.class,
              "Type as field, map/set/list as field are not supported");
    }
  }

  @Nested
  @Order(2)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class DropTable {
    @Test
    public void dropTypeSuccess() {
      // create a type first
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createType("address_to_drop", Map.of("city", "text"))
          .wasSuccessful();
      assertNamespaceCommand(keyspaceName)
          .templated()
          .dropType("address_to_drop", false)
          .wasSuccessful();
    }

    @Test
    public void dropInvalidTypeIfExistsFalse() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .dropType("invalid_type", false)
          .hasSingleApiError(
              SchemaException.Code.CANNOT_DROP_UNKNOWN_TYPE,
              SchemaException.class,
              "The command attempted to drop the unknown type: invalid_type.");
    }

    @Test
    public void dropInvalidTypeWithEmptyName() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .dropType("", false)
          .hasSingleApiError(
              ErrorCodeV1.COMMAND_FIELD_INVALID,
              "field 'command.name' value \"\" not valid. Problem: must not be empty.");
    }
  }

  @Nested
  @Order(3)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class AlterType {
    @Test
    @Order(1)
    public void alterTypeSuccess() {
      // create a type first
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createType("address_to_alter", Map.of("city", "text", "zip", "text"))
          .wasSuccessful();
      // add a new field called street
      // rename zip field to zipcode
      assertNamespaceCommand(keyspaceName)
          .templated()
          .alterType("address_to_alter", Map.of("street", "text"), Map.of("zip", "zipcode"))
          .wasSuccessful();
    }

    @Test
    public void addExistingField() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .alterType("address_to_alter", Map.of("city", "int"), Map.of())
          .hasSingleApiError(
              SchemaException.Code.CANNOT_ADD_EXISTING_FIELD,
              SchemaException.class,
              "Field name must be unique in the type");
    }

    @Test
    public void renameNotExistingField() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .alterType("address_to_alter", Map.of(), Map.of("abc", "def"))
          .hasSingleApiError(
              SchemaException.Code.CANNOT_RENAME_UNKNOWN_TYPE_FIELD,
              SchemaException.class,
              "The command attempted to rename a field that is not defined in the type");
    }
  }

  @Nested
  @Order(4)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  // Note, AlterTable shares same logic for handling UDT colum  n, won't test it here.
  class CreateTable {

    @Test
    public void shortFormNotSupported() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              "short_form_udt",
              Map.ofEntries(Map.entry("id", "text"), Map.entry("address", "addressType")),
              "id")
          .hasSingleApiError(
              SchemaException.Code.UNKNOWN_PRIMITIVE_DATA_TYPE,
              SchemaException.class,
              "The column definition used the short hand for the data type, which only supports primitive data types");
    }

    @Test
    public void unKnownUserDefinedType() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              "unknownUserDefined",
              Map.ofEntries(
                  Map.entry("id", "text"),
                  Map.entry("address", Map.of("type", "userDefined123", "udtName", "address"))),
              "id")
          .hasSingleApiError(
              SchemaException.Code.UNKNOWN_DATA_TYPE,
              SchemaException.class,
              "The column definition used a data type that is not known");
    }

    @Test
    public void unKnownUserDefinedTypeForListColumn() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              "unknownUserDefined",
              Map.ofEntries(
                  Map.entry("id", "text"),
                  Map.entry(
                      "listOfAddress",
                      Map.of(
                          "type",
                          "list",
                          "valueType",
                          Map.of("type", "userDefined123", "udtName", "address")))),
              "id")
          .hasSingleApiError(
              SchemaException.Code.UNKNOWN_DATA_TYPE,
              SchemaException.class,
              "The column definition used a data type that is not known");
    }

    @Test
    public void udtNotSupportedAsMapKey() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              "udtAsMapKey",
              Map.ofEntries(
                  Map.entry("id", "text"),
                  Map.entry(
                      "mapOfAddress",
                      Map.of(
                          "type",
                          "map",
                          "keyType",
                          Map.of("type", "userDefined", "udtName", "address"),
                          "valueType",
                          "text"))),
              "id")
          .hasSingleApiError(
              SchemaException.Code.UNSUPPORTED_MAP_DEFINITION,
              SchemaException.class,
              "Resend the command using a supported key and value type");
    }
  }

  @Nested
  @Order(5)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class WriteAndRead {

    private static final String ROW_1 =
        """
                  {
                       "id": "row_1",
                       "address": {
                           "city": "New York",
                           "postcode": "10001"
                       },
                       "listOfAddress": [
                           {
                               "city": "Los Angeles",
                               "postcode": "90001"
                           },
                           {
                               "city": "Chicago",
                               "postcode": "60601"
                           }
                       ],
                       "setOfAddress": [
                           {
                               "city": "Houston",
                               "postcode": "77001"
                           },
                           {
                               "city": "Phoenix",
                               "postcode": "85001"
                           }
                       ],
                       "mapOfAddress": {
                           "home": {
                               "city": "San Francisco",
                               "postcode": "94101"
                           },
                           "work": {
                               "city": "Seattle",
                               "postcode": "98101"
                           }
                       }
                  }
             """;

    private static final String ROW_2_TUPLE_FORMAT =
        """
                      {
                           "id": "row_2",
                           "address": [
                               ["city", "New York"],
                               ["postcode", "10001"]
                           ]
                      }
                 """;

    private static final String ROW_2_OBJECT_FORMAT =
        """
                          {
                               "id": "row_2",
                               "address": {
                                   "city": "New York",
                                   "postcode": "10001"
                               }
                          }
                     """;

    // ROW_3 is to test the update of map/set/list with UDT element
    private static final String ROW_3 =
        """
                          {
                               "id": "row_3"
                          }
                     """;

    private static final String ROW_3_UPDATED =
        """
                   {
                       "id": "row_3",
                       "listOfAddress": [
                           {
                               "city": "Boston",
                               "postcode": "02108"
                           },
                           {
                               "city": "Denver",
                               "postcode": "80201"
                           }
                       ],
                       "setOfAddress": [
                           {
                               "city": "Austin",
                               "postcode": "73301"
                           },
                           {
                               "city": "Atlanta",
                               "postcode": "30301"
                           }
                       ],
                       "mapOfAddress": {
                           "vacation": {
                               "city": "Miami",
                               "postcode": "33101"
                           },
                           "college": {
                               "city": "Madison",
                               "postcode": "53703"
                           }
                       }
                   }
               """;

    @Test
    @Order(1)
    public void insertRow1() {
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .insertOne(ROW_1)
          .wasSuccessful()
          .hasInsertedIds(List.of("row_1"));
    }

    @Test
    @Order(1)
    public void insertRow2TupleFormatUdt() {
      // TODO UDT_TODO UDT tuple format for map/set/list
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .insertOne(ROW_2_TUPLE_FORMAT)
          .wasSuccessful()
          .hasInsertedIds(List.of("row_2"));
    }

    @Test
    @Order(2)
    public void readRow1ById() {
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          // project all columns
          .find(Map.of("id", "row_1"), null, null, null)
          .wasSuccessful()
          .hasDocumentInPosition(0, ROW_1);
    }

    @Test
    @Order(2)
    public void readRow2ById() {
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          // project all columns
          .find(Map.of("id", "row_2"), null, null, null)
          .wasSuccessful()
          .hasDocumentInPosition(0, ROW_2_OBJECT_FORMAT);
    }

    @Test
    public void fieldNameMustBeString() {
      // when use tuple format for UDT
      // the field name must be a string
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .insertOne(
              """
                {
                     "id": "row_3",
                     "address": [
                         [1, "New York"],
                         ["postcode", "10001"]
                     ]
                }
                """)
          .hasSingleApiError(
              DocumentException.Code.INVALID_COLUMN_VALUES,
              DocumentException.class,
              "UDT field name must be a string");
    }

    @Test
    public void updateFullOnUdtColumn() {
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .updateOne(
              Map.of("id", "row_1"),
              Map.of(
                  "$set",
                  Map.of(
                      "address",
                      Map.of(
                          "city", "Los Angeles",
                          "postcode", "90001")))) // update full UDT column
          .wasSuccessful();

      // check if the update was successful
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .findOne(Map.of("id", "row_1"), null, null, null)
          .wasSuccessful()
          .hasJSONField(
              "data.document.address",
              """
                        {
                          "city": "Los Angeles",
                          "postcode": "90001"
                        }
                        """);

      // update back to original value
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .updateOne(
              Map.of("id", "row_1"),
              Map.of(
                  "$set",
                  Map.of(
                      "address",
                      Map.of(
                          "city", "New York",
                          "postcode", "10001")))) // update full UDT column
          .wasSuccessful();
    }

    @Test
    public void updateCollectionOfUdt() {

      // insert row_3 first, notice list/set/map are all empty
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .insertOne(ROW_3)
          .wasSuccessful()
          .hasInsertedIds(List.of("row_3"));
      // push without $each
      var pushJSON =
          """
                  {
                    "$push": {
                        "listOfAddress": {
                            "city": "Boston",
                            "postcode": "02108"
                        },
                        "setOfAddress": {
                            "city": "Austin",
                            "postcode": "73301"
                        },
                        "mapOfAddress": {
                            "vacation": {
                                "city": "Miami",
                                "postcode": "33101"
                            }
                        }
                    }
                  }
              """;

      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .updateOne(ROW_3, pushJSON)
          .wasSuccessful();

      // push with $each
      var pushEachJSON =
          """
                 {
                           "$push": {
                               "listOfAddress": {
                                   "$each": [
                                       {
                                           "city": "Denver",
                                           "postcode": "80201"
                                       }
                                   ]
                               },
                               "setOfAddress": {
                                   "$each": [
                                       {
                                           "city": "Atlanta",
                                           "postcode": "30301"
                                       }
                                   ]
                               },
                               "mapOfAddress": {
                                   "$each": [
                                       {
                                           "college": {
                                               "city": "Madison",
                                               "postcode": "53703"
                                           }
                                       }
                                   ]
                               }
                           }
                         }
              """;

      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .updateOne(ROW_3, pushEachJSON)
          .wasSuccessful();

      // after two batches of update, row_3 should have all fields populated
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .find(Map.of("id", "row_3"), null, null, null)
          .wasSuccessful()
          .hasDocumentInPosition(0, ROW_3_UPDATED);

      // then lets test some pullAll and Unset for UDT collection
      var removeEverything =
          """
                      {
                          "$pullAll": {
                              "listOfAddress": [
                                  {
                                      "city": "Boston",
                                      "postcode": "02108"
                                  },
                                  {
                                      "city": "Denver",
                                      "postcode": "80201"
                                  }
                              ],
                              "mapOfAddress": [
                                  "vacation",
                                  "college"
                              ]
                          },
                          "$unset": {
                              "setOfAddress": 1
                          }
                      }
               """;

      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .updateOne(ROW_3, removeEverything)
          .wasSuccessful();

      // after pullAll and unset, row_3 should be back to only id state
      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .find(Map.of("id", "row_3"), null, null, null)
          .wasSuccessful()
          .hasDocumentInPosition(0, ROW_3);
    }
  }
}
