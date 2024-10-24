package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiResponseValidator;
import io.stargate.sgv2.jsonapi.api.v1.util.TestDataScenarios;
import io.stargate.sgv2.jsonapi.fixtures.types.ApiDataTypesForTesting;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import io.stargate.sgv2.jsonapi.service.schema.tables.PrimitiveApiDataTypeDef;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
/**
 * Tests that read commands return a {@link
 * io.stargate.sgv2.jsonapi.api.model.command.CommandStatus#PROJECTION_SCHEMA} status field in the
 * response.
 */
public class ProjectionSchemaIntegrationTest extends AbstractTableIntegrationTestBase {

  static final String TABLE_NAME = "projectionSchemaTable";

  @BeforeAll
  public final void createDefaultTables() {
    TEST_DATA_SCENARIOS.allScalarTypesRowsWithNulls(keyspaceName, TABLE_NAME);
  }

  private static Stream<Arguments> findWithProjectionSchemaTests() {

    var commands = List.of(Command.CommandName.FIND, Command.CommandName.FIND_ONE);
    // we ask to project columns of these types
    List<List<PrimitiveApiDataTypeDef>> typeCombinations =
        List.of(
            List.of(), // for the default no projection
            ApiDataTypesForTesting.ALL_SCALAR_TYPES_FOR_CREATE,
            List.of(ApiDataTypeDefs.DURATION),
            List.of(ApiDataTypeDefs.TEXT, ApiDataTypeDefs.DURATION),
            List.of(ApiDataTypeDefs.TEXT, ApiDataTypeDefs.DURATION, ApiDataTypeDefs.INT));

    var combinations = new ArrayList<Arguments>();
    for (var command : commands) {
      for (var types : typeCombinations) {

        var columns =
            types.stream()
                .collect(Collectors.toMap(TestDataScenarios::columnName, Function.identity()));
        combinations.add(Arguments.of(command, columns));
      }

      // specials
      combinations.add(
          Arguments.of(command, Map.of(TestDataScenarios.ID_COL, ApiDataTypeDefs.TEXT)));
    }

    return combinations.stream();
  }

  @ParameterizedTest
  @MethodSource("findWithProjectionSchemaTests")
  public void findFilterWithProjectionSchema(
      Command.CommandName commandName, Map<String, PrimitiveApiDataTypeDef> columns) {

    // row-1 is the row with all the values set
    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(commandName, Map.of("id", "row-1"), columns.keySet().stream().toList())
            .wasSuccessful()
            .hasProjectionSchema();

    if (commandName == Command.CommandName.FIND) {
      validator.hasDocuments(1);
    } else if (commandName == Command.CommandName.FIND_ONE) {
      validator.hasSingleDocument();
    } else {
      throw new IllegalArgumentException("Unexpected command name: " + commandName);
    }

    assertProjectionSchema(validator, columns);
  }

  @ParameterizedTest
  @MethodSource("findWithProjectionSchemaTests")
  public void findNoFilterWithProjectionSchema(
      Command.CommandName commandName, Map<String, PrimitiveApiDataTypeDef> columns) {

    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(commandName, Map.of(), columns.keySet().stream().toList())
            .wasSuccessful()
            .hasProjectionSchema();

    if (commandName == Command.CommandName.FIND) {
      validator.hasDocuments(ApiDataTypesForTesting.ALL_SCALAR_TYPES_FOR_CREATE.size() + 1);
    } else if (commandName == Command.CommandName.FIND_ONE) {
      validator.hasSingleDocument();
    } else {
      throw new IllegalArgumentException("Unexpected command name: " + commandName);
    }

    assertProjectionSchema(validator, columns);
  }

  private DataApiResponseValidator assertProjectionSchema(
      DataApiResponseValidator validator, Map<String, PrimitiveApiDataTypeDef> columns) {
    columns.forEach(validator::hasProjectionSchemaWith);

    return validator;
  }
}
