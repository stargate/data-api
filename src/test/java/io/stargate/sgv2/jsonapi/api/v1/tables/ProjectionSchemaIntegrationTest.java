package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiResponseValidator;
import io.stargate.sgv2.jsonapi.api.v1.util.scenarios.AllScalarTypesTableScenario;
import io.stargate.sgv2.jsonapi.exception.ProjectionException;
import io.stargate.sgv2.jsonapi.fixtures.types.ApiDataTypesForTesting;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
/**
 * Tests that read commands return a {@link
 * io.stargate.sgv2.jsonapi.api.model.command.CommandStatus#PROJECTION_SCHEMA} status field in the
 * response.
 */
public class ProjectionSchemaIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final String TABLE_NAME = "projectionSchemaTable";
  private static final AllScalarTypesTableScenario TEST_DATA_SCENARIO =
      new AllScalarTypesTableScenario(keyspaceName, TABLE_NAME);

  @BeforeAll
  public final void createScenario() {
    TEST_DATA_SCENARIO.create();
  }

  @AfterAll
  public final void dropScenario() {
    TEST_DATA_SCENARIO.drop();
  }

  private static Stream<Arguments> findWithProjectionSchemaTests() {

    var commands = List.of(CommandName.FIND, CommandName.FIND_ONE);
    // we ask to project these columns
    List<ApiColumnDefContainer> projections =
        List.of(
            ApiColumnDefContainer.of(), // for the default no projection
            TEST_DATA_SCENARIO.nonPkColumns,
            ApiColumnDefContainer.of(TEST_DATA_SCENARIO.columnForDatatype(ApiDataTypeDefs.TEXT)),
            ApiColumnDefContainer.of(
                TEST_DATA_SCENARIO.columnForDatatype(ApiDataTypeDefs.TEXT),
                TEST_DATA_SCENARIO.columnForDatatype(ApiDataTypeDefs.DURATION)),
            ApiColumnDefContainer.of(
                TEST_DATA_SCENARIO.columnForDatatype(ApiDataTypeDefs.TEXT),
                TEST_DATA_SCENARIO.columnForDatatype(ApiDataTypeDefs.DURATION),
                TEST_DATA_SCENARIO.columnForDatatype(ApiDataTypeDefs.INT)));

    var combinations = new ArrayList<Arguments>();
    for (var command : commands) {
      for (var projection : projections) {
        combinations.add(Arguments.of(command, projection));
      }

      // specials
      combinations.add(
          Arguments.of(command, ApiColumnDefContainer.of(TEST_DATA_SCENARIO.primaryKey)));
    }

    return combinations.stream();
  }

  @ParameterizedTest
  @MethodSource("findWithProjectionSchemaTests")
  public void findFilterWithProjectionSchema(
      CommandName commandName, ApiColumnDefContainer projectionColumns) {

    // row-1 is the row with all the values set
    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(
                commandName,
                Map.of("id", "row-1"),
                projectionColumns.keySet().stream().map(CqlIdentifier::asInternal).toList())
            .wasSuccessful()
            .hasProjectionSchema();

    if (commandName == CommandName.FIND) {
      validator.hasDocuments(1);
    } else if (commandName == CommandName.FIND_ONE) {
      validator.hasSingleDocument();
    } else {
      throw new IllegalArgumentException("Unexpected command name: " + commandName);
    }

    assertProjectionSchema(validator, projectionColumns);
  }

  @ParameterizedTest
  @MethodSource("findWithProjectionSchemaTests")
  public void findNoFilterWithProjectionSchema(
      CommandName commandName, ApiColumnDefContainer projectionColumns) {

    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(
                commandName,
                Map.of(),
                projectionColumns.keySet().stream().map(CqlIdentifier::asInternal).toList())
            .wasSuccessful()
            .hasProjectionSchema();

    if (commandName == CommandName.FIND) {
      validator.hasDocuments(ApiDataTypesForTesting.ALL_SCALAR_TYPES_FOR_CREATE.size() + 1);
    } else if (commandName == CommandName.FIND_ONE) {
      validator.hasSingleDocument();
    } else {
      throw new IllegalArgumentException("Unexpected command name: " + commandName);
    }

    assertProjectionSchema(validator, projectionColumns);
  }

  @Test
  public void findManyProjectionMissingColumns() {

    // Select a column that does not exist, should not be in the projection schema
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(Map.of("id", "row-1"), List.of("id", "MISSING_COLUMN"), Map.of(), Map.of())
        .wasSuccessful()
        .hasProjectionSchema()
        .hasDocuments(1)
        .hasProjectionSchemaWith("id", ApiDataTypeDefs.TEXT)
        .hasSingleApiError(
            ProjectionException.Code.UNKNOWN_TABLE_COLUMNS,
            ProjectionException.class,
            "The projection included the following unknown columns: [MISSING_COLUMN]");
  }

  @Test
  public void findProjectionUnknownColumns() {

    // Select a column that does not exist, should not be in the projection schema
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(Map.of("id", "row-1"), List.of("FAKE2", "FAKE1"), Map.of(), Map.of())
        .hasSingleApiError(
            ProjectionException.Code.UNKNOWN_TABLE_COLUMNS,
            ProjectionException.class,
            "The projection included the following unknown columns: [FAKE2, FAKE1]");
  }

  private DataApiResponseValidator assertProjectionSchema(
      DataApiResponseValidator validator, ApiColumnDefContainer columns) {
    columns.values().forEach(validator::hasProjectionSchemaWith);

    return validator;
  }
}
