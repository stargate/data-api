package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
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
  class AlterTable {
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
}
