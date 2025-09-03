package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
public class ListTypesIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final String SUPPORTED_UDT = "supported_udt";

  private static final String UNSUPPORTED_UDT_COLLECTION_FIELD = "udt_with_collection_field";

  private static final String UNSUPPORTED_UDT_NESTED_FIELD = "udt_with_nested_udt_field";

  @BeforeAll
  public void setupTypes() {
    executeCqlStatement(
            """
                CREATE TYPE IF NOT EXISTS "%s"."%s" (
                    "textField" text,
                    "intField" int,
                    "durationField" duration,
                    "timeuuidField" timeuuid,
                    "blobField" blob
                )
            """
            .formatted(keyspaceName, SUPPORTED_UDT));

    executeCqlStatement(
            """
                CREATE TYPE IF NOT EXISTS "%s"."%s" (
                    "listField" list<text> ,
                    "setField" set<text> ,
                    "mapField" map<text, text>
                )
                """
            .formatted(keyspaceName, UNSUPPORTED_UDT_COLLECTION_FIELD));

    executeCqlStatement(
            """
                CREATE TYPE IF NOT EXISTS "%s"."%s" (
                    "nestedField" "%s"
                )
                """
            .formatted(keyspaceName, UNSUPPORTED_UDT_NESTED_FIELD, SUPPORTED_UDT));
  }

  @Test
  public void shouldListTypesWithNoExplain() {
    assertNamespaceCommand(keyspaceName)
        .templated()
        .listTypes(false)
        .wasSuccessful()
        // Validate that status.types is not null
        .body("status.types", notNullValue())
        // Validate the number of types in the response
        .body("status.types", hasSize(3))
        .body(
            "status.types",
            containsInAnyOrder(
                equalTo(SUPPORTED_UDT),
                equalTo(UNSUPPORTED_UDT_COLLECTION_FIELD),
                equalTo(UNSUPPORTED_UDT_NESTED_FIELD)));
  }

  @Test
  public void shouldListTypesWithExplain() {

    String explain_supported_udt =
            """
                {
                  "type": "userDefined",
                  "udtName": "supported_udt",
                  "definition": {
                    "fields": {
                      "textField": {
                        "type": "text"
                      },
                      "intField": {
                        "type": "int"
                      },
                      "durationField": {
                        "type": "duration"
                      },
                      "timeuuidField": {
                        "type": "timeuuid",
                        "apiSupport": {
                          "createTable": false,
                          "insert": true,
                          "read": true,
                          "filter": true,
                          "cqlDefinition": "timeuuid"
                        }
                      },
                      "blobField": {
                        "type": "blob"
                      }
                    }
                  },
                  "apiSupport": {
                    "createTable": true,
                    "insert": true,
                    "read": true,
                    "filter": false,
                    "cqlDefinition": "\\"%s\\".%s"
                  }
                }
                """
            .formatted(keyspaceName, SUPPORTED_UDT);

    String explain_unsupported_udt_collection_field =
            """
                {
                        "type": "UNSUPPORTED",
                        "apiSupport": {
                          "createTable": false,
                          "insert": false,
                          "read": false,
                          "filter": false,
                          "cqlDefinition": "\\"%s\\".%s"
                        }
                      }
                """
            .formatted(keyspaceName, UNSUPPORTED_UDT_COLLECTION_FIELD);

    String explain_unsupported_udt_nested_field =
            """
                {
                        "type": "UNSUPPORTED",
                        "apiSupport": {
                          "createTable": false,
                          "insert": false,
                          "read": false,
                          "filter": false,
                          "cqlDefinition": "\\"%s\\".%s"
                        }
                      }
                """
            .formatted(keyspaceName, UNSUPPORTED_UDT_NESTED_FIELD);

    assertNamespaceCommand(keyspaceName)
        .templated()
        .listTypes(true)
        .wasSuccessful()
        // Validate that status.types is not null
        .body("status.types", notNullValue())
        // Validate the number of types in the response
        .body("status.types", hasSize(3))
        .body(
            "status.types",
            contains(
                jsonEquals(explain_supported_udt),
                jsonEquals(explain_unsupported_udt_collection_field),
                jsonEquals(explain_unsupported_udt_nested_field)));
  }
}
