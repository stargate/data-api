package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class DropTypeIntegrationTest extends AbstractTableIntegrationTestBase {
  @Test
  public void dropTypeSuccess() {
    // create a type first
    assertNamespaceCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName)
        .templated()
        .createType("address_to_drop", Map.of("city", "text"))
        .wasSuccessful();
    assertNamespaceCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName)
        .templated()
        .dropType("address_to_drop", false)
        .wasSuccessful();
  }

  @Test
  public void dropInvalidTypeIfExistsFalse() {
    assertNamespaceCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName)
        .templated()
        .dropType("invalid_type", false)
        .hasSingleApiError(
            SchemaException.Code.CANNOT_DROP_UNKNOWN_TYPE,
            SchemaException.class,
            "The command attempted to drop the unknown type: invalid_type.");
  }

  @Test
  public void dropInvalidTypeWithEmptyName() {
    assertNamespaceCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName)
        .templated()
        .dropType("", false)
        .hasSingleApiError(
            ErrorCodeV1.COMMAND_FIELD_INVALID,
            "field 'command.name' value \"\" not valid. Problem: must not be empty.");
  }
}
