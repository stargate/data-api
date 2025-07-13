package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
class AlterTypeIntegrationTest extends AbstractTableIntegrationTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlterTypeIntegrationTest.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String tableName(String typeName){
    return "table_for_" + typeName;
  }
  private void createTestTypeAndTable(String typeName){
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

    LOGGER.info("Creating test typeName: {}, tableName: {}", typeName, tableName(typeName));
    assertNamespaceCommand(keyspaceName).templated().createType(typeName, fields).wasSuccessful();

    // TODO: NO LIST TYPE, WE CANNOT VERIFY THE TYPE WAS CREATED, SO USING CREATE TABLE
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            tableName(typeName),
            Map.of("id", "text", "udt", Map.of("type", "userDefined", "udtName", typeName)),
            "id")
        .wasSuccessful();

    // TODO: NO LIST TYPE, WE CANNOT VERIFY THE TYPE WAS CREATED AS DEFINED, SO USING READ TABLE TO
    // GET SCHEMA
    assertTableCommand(keyspaceName, tableName(typeName))
        .templated()
        .findOne(Map.of(), List.of())
        .wasSuccessful()
        .hasJSONField("status.projectionSchema.udt.definition.fields", fields);
  }

  private void assertAlter(String typeName, String alterOp, String matchFields) {
    LOGGER.info("Assert Alter Type for typeName: {}, alterOp: {}", typeName, alterOp);

    createTestTypeAndTable(typeName);

    assertNamespaceCommand(keyspaceName).templated().alterType(typeName, alterOp).wasSuccessful();

    // TODO: NO LIST TYPE, WE CANNOT VERIFY THE TYPE WAS MODIFIED, SO USING READ TABLE TO GET SCHEMA
    assertTableCommand(keyspaceName, tableName(typeName))
        .templated()
        .findOne(Map.of(), List.of())
        .wasSuccessful()
        .hasJSONField("status.projectionSchema.udt.definition.fields", matchFields);
  }

  private  <T extends APIException>  void assertAlterFails(String typeName, String alterOp,
                                                           ErrorCode<T> errorCode,
                                                           Class<T> exceptionClass,
                                                           String... errorMessage) {
    LOGGER.info("Assert Failing Alter Type for typeName: {}, alterOp: {}", typeName, alterOp);

    createTestTypeAndTable(typeName);

    assertNamespaceCommand(keyspaceName)
        .templated()
        .alterType(typeName, alterOp)
        .hasSingleApiError(
          errorCode,
          exceptionClass,
          errorMessage);
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

    assertAlterFails("renameMissingField", alterOp,
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

    assertAlterFails("renameExistingField", alterOp,
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

    assertAlterFails("addExistingField", alterOp,
        SchemaException.Code.CANNOT_ADD_EXISTING_FIELD,
        SchemaException.class,
        "The existing field name was: city.");
  }
}
