package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class DropTypeIntegrationTest extends TypeIntegrationTestBase {

  private static final String FIELDS =
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

  @Test
  public void dropTypeSuccess() {

    createTypeAndTable("dropTypeSuccess", FIELDS);
    dropTypeTable("dropTypeSuccess");
    assertNamespaceCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName)
        .templated()
        .dropType("dropTypeSuccess", false)
        .wasSuccessful();
  }

  @Test
  public void dropInUseType() {

    createTypeAndTable("dropInUseType", FIELDS);

    // See the DropTypeExceptionHandler, the error message is different for DSE and HCD
    // so only validating it has the table name in the error message
    assertNamespaceCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName)
        .templated()
        .dropType("dropInUseType", false)
        .hasSingleApiError(
            SchemaException.Code.CANNOT_DROP_TYPE_USED_BY_TABLE,
            SchemaException.class,
            "The command attempted to drop the type: \"dropInUseType\"",
            tableName("dropInUseType"));
  }

  @Test
  public void dropUnknownTypeIfExistsFalse() {

    assertNamespaceCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName)
        .templated()
        .dropType("dropUnknownTypeIfExistsFalse", false)
        .hasSingleApiError(
            SchemaException.Code.CANNOT_DROP_UNKNOWN_TYPE,
            SchemaException.class,
            "The command attempted to drop the unknown type: \"dropUnknownTypeIfExistsFalse\".");
  }

  @Test
  public void dropUnknownTypeIfExistsTrue() {

    assertNamespaceCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName)
        .templated()
        .dropType("dropUnknownTypeIfExistsTrue", true)
        .wasSuccessful();
  }

  @Test
  public void dropInvalidTypeWithEmptyName() {

    assertNamespaceCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName)
        .templated()
        .dropType("", false)
        .hasSingleApiError(
            RequestException.Code.COMMAND_FIELD_INVALID,
            RequestException.class,
            "field 'command.name' value \"\" not valid.",
            "Problem: must not be empty.");
  }
}
