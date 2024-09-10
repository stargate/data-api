package io.stargate.sgv2.jsonapi.api.v1.tables;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateTableIntegrationTest extends AbstractTableIntegrationTestBase {
  @Nested
  @Order(1)
  class CreateTableOk {
    @Test
    public void primaryKeyAsString() {
      String tableDef =
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
                    """;
      // createTable() validates command succeeds and response is OK:
      createTable(tableDef);
      deleteTable("primaryKeyAsStringTable");
    }

    @Test
    public void primaryKeyAsJsonObject() {

      String tableDef =
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
                    """;
      createTable(tableDef);
      deleteTable("primaryKeyAsJsonObjectTable");
    }
  }
}
