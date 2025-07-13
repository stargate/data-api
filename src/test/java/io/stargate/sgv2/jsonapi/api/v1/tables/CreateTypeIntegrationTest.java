package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase;
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
class CreateTypeIntegrationTest extends AbstractTableIntegrationTestBase {

  private static Stream<Arguments> unsupportedUDTFields() {
    return Stream.of(
        // list/set/map as field are not supported
        Arguments.of(
            Map.of("listField", Map.of("type", "list", "valueType", "text")),
            "The command has contained the unsupported types: list."),
        // nested UDT as field is not supported
        Arguments.of(
            Map.of("address", Map.of("type", "userDefined", "udtName", "address")),
            "The command has contained the unsupported types: userDefined."));
  }

  @ParameterizedTest
  @MethodSource("unsupportedUDTFields")
  public void unsupportedUDTField(Map<String, Object> fields, String errorMessage) {
    assertNamespaceCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName)
        .templated()
        .createType("type_with_supported_filed", fields)
        .hasSingleApiError(
            SchemaException.Code.UNSUPPORTED_TYPE_FIELDS, SchemaException.class, errorMessage);
  }
}
