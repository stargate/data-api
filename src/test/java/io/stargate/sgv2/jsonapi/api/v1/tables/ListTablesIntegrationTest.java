package io.stargate.sgv2.jsonapi.api.v1.tables;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class ListTablesIntegrationTest extends AbstractTableIntegrationTestBase {
  @BeforeAll
  public final void createDefaultTables() {
    String tableData =
        """
                    {
                        "name": "allTypesTable",
                        "definition": {
                            "columns": {
                                "ascii_type": "ascii",
                                "bigint_type": "bigint",
                                "blob_type": "blob",
                                "boolean_type": "boolean",
                                "date_type": "date",
                                "decimal_type": "decimal",
                                "double_type": "double",
                                "duration_type": "duration",
                                "float_type": "float",
                                "inet_type": "inet",
                                "int_type": "int",
                                "smallint_type": "smallint",
                                "text_type": "text",
                                "time_type": "time",
                                "timestamp_type": "timestamp",
                                "tinyint_type": "tinyint",
                                "uuid_type": "uuid",
                                "varint_type": "varint",
                                "map_type": {
                                    "type": "map",
                                    "keyType": "text",
                                    "valueType": "int"
                                },
                                "list_type": {
                                    "type": "list",
                                    "valueType": "text"
                                },
                                "set_type": {
                                    "type": "set",
                                    "valueType": "text"
                                },
                                "vector_type": {
                                    "type": "vector",
                                    "dimension": 1024,
                                    "service": {
                                        "provider": "mistral",
                                        "modelName": "mistral-embed"
                                    }
                                }
                            },
                            "primaryKey": {
                                "partitionBy": [
                                    "text_type"
                                ],
                                "partitionSort": {
                                    "int_type": 1,
                                    "bigint_type": -1
                                }
                            }
                        },
                        "options": {
                            "ifNotExists": true
                        }
                    }
                    """;
    createTable(tableData);
    String table2 =
        """
                    {
                        "name": "person",
                        "definition": {
                            "columns": {
                                "id": "text",
                                "age": "int",
                                "name": "text",
                                "city": "text"
                            },
                            "primaryKey": "id"
                        }
                    }
                """;
    createTable(table2);
  }

  @Nested
  @Order(1)
  class ListTables {

    @Test
    public void listTablesOnly() {
      String listTablesOnly =
          """
                    {}
                    """;
      listTables(listTablesOnly)
          .hasNoErrors()
          // Validate that status.tables is not null
          .body("status.tables", notNullValue())

          // Validate the number of tables in the response
          .body("status.tables", hasSize(2))

          // Validate the specific table names in the position
          .body("status.tables[0]", equalTo("allTypesTable"))
          .body("status.tables[1]", equalTo("person"));
    }

    @Test
    public void listTablesWithSchema() {
      String listTablesWithSchema =
          """
              {
                "options" : {
                  "explain" : true
                }
              }
              """;
      listTables(listTablesWithSchema)
          .hasNoErrors()
          // Validate that status.tables is not null and contains one table: allTypesTable
          .body("status.tables", notNullValue())
          .body("status.tables", hasSize(2))
          .body("status.tables[0].name", equalTo("allTypesTable"))

          // Validate that the table contains the expected columns and types
          .body("status.tables[0].definition.columns.date_type.type", equalTo("date"))
          .body("status.tables[0].definition.columns.time_type.type", equalTo("time"))
          .body("status.tables[0].definition.columns.text_type.type", equalTo("text"))
          .body("status.tables[0].definition.columns.int_type.type", equalTo("int"))
          .body("status.tables[0].definition.columns.vector_type.type", equalTo("vector"))
          .body(
              "status.tables[0].definition.columns.vector_type.dimension",
              equalTo(1024)) // Additional dimension check for vector type
          .body(
              "status.tables[0].definition.columns.vector_type.service.provider",
              equalTo("mistral"))
          .body(
              "status.tables[0].definition.columns.vector_type.service.modelName",
              equalTo("mistral-embed"))
          .body("status.tables[0].definition.columns.duration_type.type", equalTo("duration"))
          .body("status.tables[0].definition.columns.timestamp_type.type", equalTo("timestamp"))
          .body("status.tables[0].definition.columns.set_type.type", equalTo("set"))
          .body(
              "status.tables[0].definition.columns.set_type.valueType",
              equalTo("text")) // Set's valueType check
          .body("status.tables[0].definition.columns.bigint_type.type", equalTo("bigint"))
          .body("status.tables[0].definition.columns.boolean_type.type", equalTo("boolean"))
          .body("status.tables[0].definition.columns.uuid_type.type", equalTo("uuid"))
          .body("status.tables[0].definition.columns.blob_type.type", equalTo("blob"))
          .body("status.tables[0].definition.columns.inet_type.type", equalTo("inet"))
          .body("status.tables[0].definition.columns.list_type.type", equalTo("list"))
          .body(
              "status.tables[0].definition.columns.list_type.valueType",
              equalTo("text")) // List's valueType check
          .body("status.tables[0].definition.columns.map_type.type", equalTo("map"))
          .body(
              "status.tables[0].definition.columns.map_type.keyType",
              equalTo("text")) // Map's keyType check
          .body(
              "status.tables[0].definition.columns.map_type.valueType",
              equalTo("int")) // Map's valueType check
          .body("status.tables[0].definition.columns.varint_type.type", equalTo("varint"))
          .body("status.tables[0].definition.columns.tinyint_type.type", equalTo("tinyint"))
          .body("status.tables[0].definition.columns.decimal_type.type", equalTo("decimal"))
          .body("status.tables[0].definition.columns.float_type.type", equalTo("float"))
          .body("status.tables[0].definition.columns.ascii_type.type", equalTo("ascii"))
          .body("status.tables[0].definition.columns.double_type.type", equalTo("double"))
          .body("status.tables[0].definition.columns.smallint_type.type", equalTo("smallint"))

          // Validate the primary key;
          .body("status.tables[0].definition.primaryKey.partitionBy[0]", equalTo("text_type"))
          .body("status.tables[0].definition.primaryKey.partitionSort.int_type", equalTo(1))
          .body("status.tables[0].definition.primaryKey.partitionSort.bigint_type", equalTo(-1));
    }
  }
}
