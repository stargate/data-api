package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateTypeIntegrationTest extends TypeIntegrationTestBase {

  private void assertCreateType(String typeName, String fields, String matchFields) {
    createTypeAndTable(typeName, fields, matchFields);
  }

  private <T extends APIException> void assertCreateFails(
      String typeName,
      String fields,
      ErrorCode<T> errorCode,
      Class<T> exceptionClass,
      String... errorMessage) {
    assertNamespaceCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName)
        .templated()
        .createType(typeName, fields)
        .hasSingleApiError(errorCode, exceptionClass, errorMessage);
  }

  @ParameterizedTest
  @MethodSource("supportedUdtTests")
  public void supportedUdt(String typeName, String fields, String matchFields) {

    assertCreateType(typeName, fields, matchFields);
  }

  private static Stream<Arguments> supportedUdtTests() {
    var allShortForm =
        """
            {
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
             "varint_type": "varint"
            }""";

    var allLongForm =
        """
                {
                  "ascii_type": {
                    "type": "ascii"
                  },
                  "bigint_type": {
                    "type": "bigint"
                  },
                  "blob_type": {
                    "type": "blob"
                  },
                  "boolean_type": {
                    "type": "boolean"
                  },
                  "date_type": {
                    "type": "date"
                  },
                  "decimal_type": {
                    "type": "decimal"
                  },
                  "double_type": {
                    "type": "double"
                  },
                  "duration_type": {
                    "type": "duration"
                  },
                  "float_type": {
                    "type": "float"
                  },
                  "inet_type": {
                    "type": "inet"
                  },
                  "int_type": {
                    "type": "int"
                  },
                  "smallint_type": {
                    "type": "smallint"
                  },
                  "text_type": {
                    "type": "text"
                  },
                  "time_type": {
                    "type": "time"
                  },
                  "timestamp_type": {
                    "type": "timestamp"
                  },
                  "tinyint_type": {
                    "type": "tinyint"
                  },
                  "uuid_type": {
                    "type": "uuid"
                  },
                  "varint_type": {
                    "type": "varint"
                  }
                }
            """;

    return Stream.of(
        Arguments.of("allPrimitivesShortForm", allShortForm, allLongForm),
        Arguments.of("allPrimitivesLongForm", allLongForm, allLongForm));
  }

  @ParameterizedTest
  @MethodSource("unsupportedUDTFields")
  public void unsupportedUDTField(String typeName, String fields, String errorMessage) {

    assertCreateFails(
        typeName,
        fields,
        SchemaException.Code.UNSUPPORTED_TYPE_FIELDS,
        SchemaException.class,
        errorMessage);
  }

  private static Stream<Arguments> unsupportedUDTFields() {
    return Stream.of(
        Arguments.of(
            "listFieldsNotSupported",
            """
                {
                  "listField": {
                    "type": "list",
                    "valueType": "text"
                  }
                }
                """,
            "The command used the unsupported types: list."),
        Arguments.of(
            "setFieldsNotSupported",
            """
                {
                  "setField": {
                    "type": "set",
                    "valueType": "text"
                  }
                }
                """,
            "The command used the unsupported types: set."),
        Arguments.of(
            "mapFieldsNotSupported",
            """
                {
                  "mapField": {
                    "type": "map",
                    "keyType": "text",
                    "valueType": "text"
                  }
                }
                """,
            "The command used the unsupported types: map."),
        Arguments.of(
            "nestedUDTFieldNotSupported",
            """
                {
                  "nestedUDT": {
                    "type": "userDefined",
                    "udtName": "address"
                  }
                }
                """,
            "The command used the unsupported types: userDefined."),
        Arguments.of(
            "vectorFieldNotSupported",
            """
                {
                  "vectorField": {
                    "type": "vector",
                    "dimension": 5
                  }
                }
                """,
            "The command used the unsupported types: vector."));
  }
}
