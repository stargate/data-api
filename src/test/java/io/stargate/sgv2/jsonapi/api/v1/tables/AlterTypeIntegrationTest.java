package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
class AlterTypeIntegrationTest extends TypeIntegrationTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlterTypeIntegrationTest.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private void createTypeAndTable(String typeName) {
    var fields =
        """
        {
          "city": {
            "type": "text"
          },
          "zip": {
            "type": "text"
          }
        }
        """;
    createTypeAndTable(typeName, fields);
  }

  private void assertAlter(String typeName, String alterOp, String matchFields) {
    LOGGER.info("Assert Alter Type for typeName: {}, alterOp: {}", typeName, alterOp);

    createTypeAndTable(typeName);
    assertNamespaceCommand(keyspaceName).templated().alterType(typeName, alterOp).wasSuccessful();
    assertType(typeName, matchFields);
  }

  private <T extends APIException> void assertAlterFails(
      String typeName,
      String alterOp,
      ErrorCode<T> errorCode,
      Class<T> exceptionClass,
      String... errorMessage) {
    LOGGER.info("Assert Failing Alter Type for typeName: {}, alterOp: {}", typeName, alterOp);

    createTypeAndTable(typeName);

    assertNamespaceCommand(keyspaceName)
        .templated()
        .alterType(typeName, alterOp)
        .hasSingleApiError(errorCode, exceptionClass, errorMessage);
  }

  @Test
  public void addField() {

    var alterOp =
        """
        "add": {
            "fields": {
                "street": "text"
            }
        }
        """;

    var matchFields =
        """
        {
            "city": {
                "type": "text"
            },
            "zip": {
                "type": "text"
            },
            "street": {
                "type": "text"
            }
        }
        """;
    assertAlter("addField", alterOp, matchFields);
  }

  @Test
  public void renameField() {

    var alterOp =
        """
        "rename": {
            "fields": {
                "zip": "zipcode"
            }
        }
        """;

    var matchFields =
        """
        {
            "city": {
                "type": "text"
            },
            "zipcode": {
                "type": "text"
            }
        }
        """;
    assertAlter("renameField", alterOp, matchFields);
  }

  @Test
  public void renameMissingField() {

    var alterOp =
        """
        "rename": {
            "fields": {
                "missing_field": "zipcode"
            }
        }
        """;

    assertAlterFails(
        "renameMissingField",
        alterOp,
        SchemaException.Code.CANNOT_RENAME_UNKNOWN_TYPE_FIELD,
        SchemaException.class,
        "The unknown field was: missing_field.");
  }

  @Test
  public void renameExistingField() {

    var alterOp =
        """
        "rename": {
            "fields": {
                "city": "zip"
            }
        }
        """;

    assertAlterFails(
        "renameExistingField",
        alterOp,
        SchemaException.Code.CANNOT_ADD_EXISTING_FIELD,
        SchemaException.class,
        "The existing field name was: zip.");
  }

  @Test
  public void addExistingField() {

    var alterOp =
        """
          "add": {
              "fields": {
                  "city": "text"
              }
          }
        """;

    assertAlterFails(
        "addExistingField",
        alterOp,
        SchemaException.Code.CANNOT_ADD_EXISTING_FIELD,
        SchemaException.class,
        "The existing field name was: city.");
  }

  @Test
  public void emptyAdd() {
    var alterOp =
        """
          "add": {}
        """;

    assertAlterFails(
        "emptyAdd",
        alterOp,
        SchemaException.Code.MISSING_ALTER_TYPE_OPERATIONS,
        SchemaException.class);
  }

  @Test
  public void emptyAddWithNoFields() {
    var alterOp =
        """
              "add": {
                "fields":{
                }
              }
            """;
    assertAlterFails(
        "emptyAddWithNoFields",
        alterOp,
        SchemaException.Code.MISSING_ALTER_TYPE_OPERATIONS,
        SchemaException.class);
  }

  @Test
  public void emptyRenameField() {

    var alterOp =
        """
          "rename": {}
        """;

    assertAlterFails(
        "emptyRenameField",
        alterOp,
        SchemaException.Code.MISSING_ALTER_TYPE_OPERATIONS,
        SchemaException.class);
  }
}
