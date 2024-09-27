package io.stargate.sgv2.jsonapi.api.v1.tables;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateTableIntegrationTest extends AbstractTableIntegrationTestBase {
  @Nested
  @Order(1)
  class CreateTable {
    @ParameterizedTest
    @MethodSource("allTableData")
    public void primaryKeyAsString(CreateTableTestData testData) {
      if (testData.error()) {
        createTableErrorValidation(
            testData.request(), testData.errorCode(), testData.errorMessage());
      } else {
        createTable(testData.request());
        deleteTable(testData.tableName());
      }
    }

    private record CreateTableTestData(
        String request, String tableName, boolean error, String errorCode, String errorMessage) {}

    private static Stream<Arguments> allTableData() {
      List<Arguments> testCases = new ArrayList<>();
      // primaryKeyAsString
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                    {
                                       "name": "primaryKeyAsStringTable",
                                       "definition": {
                                           "columns": {
                                               "id": {
                                                   "type": "text"
                                               },
                                               "age": {
                                                   "type": "int"
                                               },
                                               "name": {
                                                   "type": "text"
                                               }
                                           },
                                           "primaryKey": "id"
                                       }
                                    }
                                    """,
                  "primaryKeyAsStringTable",
                  false,
                  null,
                  null)));

      // primaryKeyWithQuotable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                    {
                                          "name": "primaryKeyWithQuotable",
                                          "definition": {
                                            "primaryKey": "_id",
                                            "columns": {
                                              "_id": {
                                                "type": "text"
                                              },
                                              "name": {
                                                "type": "text"
                                              }
                                            }
                                          }
                                      }
                                    """,
                  "primaryKeyWithQuotable",
                  false,
                  null,
                  null)));

      // columnTypeusingShortHandTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                    {
                                       "name": "columnTypeusingShortHandTable",
                                       "definition": {
                                           "columns": {
                                               "id": "text",
                                               "age": "int",
                                               "name": "text"
                                           },
                                           "primaryKey": "id"
                                       }
                                    }
                                    """,
                  "columnTypeusingShortHandTable",
                  false,
                  null,
                  null)));

      // invalidPrimaryKeyTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                    {
                                     "name": "invalidPrimaryKeyTable",
                                     "definition": {
                                         "columns": {
                                             "id": {
                                                 "type": "text"
                                             },
                                             "age": {
                                                 "type": "int"
                                             },
                                             "name": {
                                                 "type": "text"
                                             }
                                         },
                                         "primaryKey": "invalid"
                                     }
                                    }
                                    """,
                  "invalidPrimaryKeyTable",
                  true,
                  "TABLE_COLUMN_DEFINITION_MISSING",
                  "Column definition is missing for the provided primary key field: invalid")));

      // primaryKeyAsJsonObjectTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                    {
                                      "name": "primaryKeyAsJsonObjectTable",
                                      "definition": {
                                        "columns": {
                                          "id": {
                                            "type": "text"
                                          },
                                          "age": {
                                            "type": "int"
                                          },
                                          "name": {
                                            "type": "text"
                                          }
                                        },
                                        "primaryKey": {
                                          "partitionBy": [
                                            "id"
                                          ],
                                          "partitionSort" : {
                                            "name" : 1, "age" : -1
                                          }
                                        }
                                      }
                                    }
                                    """,
                  "primaryKeyAsJsonObjectTable",
                  false,
                  null,
                  null)));

      // invalidPartitionByTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                    {
                                      "name": "invalidPartitionByTable",
                                      "definition": {
                                        "columns": {
                                          "id": "text",
                                          "age": "int",
                                          "name": "text"
                                        },
                                        "primaryKey": {
                                          "partitionBy": [
                                            "invalid"
                                          ],
                                          "partitionSort" : {
                                            "name" : 1, "age" : -1
                                          }
                                        }
                                      }
                                    }
                                    """,
                  "invalidPartitionByTable",
                  true,
                  "TABLE_COLUMN_DEFINITION_MISSING",
                  "Column definition is missing for the provided primary key field: invalid")));

      // invalidPartitionSortTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                    {
                                        "name": "invalidPartitionSortTable",
                                        "definition": {
                                          "columns": {
                                            "id": "text",
                                            "age": "int",
                                            "name": "text"
                                          },
                                          "primaryKey": {
                                            "partitionBy": [
                                              "id"
                                            ],
                                            "partitionSort" : {
                                              "invalid" : 1, "age" : -1
                                            }
                                          }
                                        }
                                      }
                                    """,
                  "invalidPartitionSortTable",
                  true,
                  "TABLE_COLUMN_DEFINITION_MISSING",
                  "Column definition is missing for the provided primary key field: invalid")));

      // invalidPartitionSortOrderingValueTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                    {
                                      "name": "invalidPartitionSortOrderingValueTable",
                                      "definition": {
                                        "columns": {
                                          "id": "text",
                                          "age": "int",
                                          "name": "text"
                                        },
                                        "primaryKey": {
                                          "partitionBy": [
                                            "id"
                                          ],
                                          "partitionSort" : {
                                            "id" : 1, "age" : 0
                                          }
                                        }
                                      }
                                    }
                                    """,
                  "invalidPartitionSortOrderingValueTable",
                  true,
                  "TABLE_PRIMARY_KEY_DEFINITION_INCORRECT",
                  "Primary key definition is incorrect: partitionSort value should be 1 or -1")));

      // invalidPartitionSortOrderingValueTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                    {
                                        "name": "invalidPartitionSortOrderingValueTypeTable",
                                        "definition": {
                                          "columns": {
                                            "id": "text",
                                            "age": "int",
                                            "name": "text"
                                          },
                                          "primaryKey": {
                                            "partitionBy": [
                                              "id"
                                            ],
                                            "partitionSort" : {
                                              "id" : 1, "age" : "invalid"
                                            }
                                          }
                                        }
                                      }
                                    """,
                  "invalidPartitionSortOrderingValueTypeTable",
                  true,
                  "TABLE_PRIMARY_KEY_DEFINITION_INCORRECT",
                  "Primary key definition is incorrect: partitionSort should be 1 or -1")));

      // invalidColumnTypeTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                    {
                                      "name": "invalidColumnTypeTable",
                                      "definition": {
                                        "columns": {
                                          "id": "invalid",
                                          "age": "int",
                                          "name": "text"
                                        },
                                        "primaryKey": {
                                          "partitionBy": [
                                              "id"
                                          ],
                                          "partitionSort": {
                                              "id": 1,
                                              "age": -1
                                          }
                                        }
                                      }
                                    }
                                    """,
                  "invalidColumnTypeTable",
                  true,
                  "TABLE_COLUMN_TYPE_UNSUPPORTED",
                  "Unsupported column types: Invalid column type: invalid")));
      return testCases.stream();
    }
  }
}
