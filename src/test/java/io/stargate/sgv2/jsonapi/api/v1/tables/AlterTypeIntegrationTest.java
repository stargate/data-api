package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
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

  private void assertAlter(String typeName, String alterOp, String matchFields) {
    LOGGER.info("Assert Alter Type for type: {}, alterOp: {}", typeName, alterOp);

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
    var tableName = "table_for_" + typeName;
    assertNamespaceCommand(keyspaceName).templated().createType(typeName, fields).wasSuccessful();

    // TODO: NO LIST TYPE, WE CANNOT VERIFY THE TYPE WAS CREATED, SO USING CREATE TABLE
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            tableName,
            Map.of("id", "text", "udt", Map.of("type", "userDefined", "udtName", typeName)),
            "id")
        .wasSuccessful();

    // TODO: NO LIST TYPE, WE CANNOT VERIFY THE TYPE WAS CREATED AS DEFINED, SO USING READ TABLE TO
    // GET SCHEMA
    assertTableCommand(keyspaceName, tableName)
        .templated()
        .findOne(Map.of(), List.of())
        .wasSuccessful()
        .hasJSONField("status.projectionSchema.udt.definition.fields", fields);

    assertNamespaceCommand(keyspaceName).templated().alterType(typeName, alterOp).wasSuccessful();

    // TODO: NO LIST TYPE, WE CANNOT VERIFY THE TYPE WAS MODIFIED, SO USING READ TABLE TO GET SCHEMA
    assertTableCommand(keyspaceName, tableName)
        .templated()
        .findOne(Map.of(), List.of())
        .wasSuccessful()
        .hasJSONField("status.projectionSchema.udt.definition.fields", matchFields);
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
