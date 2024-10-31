package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.v1.util.scenarios.VectorDimension5TableScenario;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class AnnSortTableIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final String TABLE_NAME = "annSortTableTest";
  private static final VectorDimension5TableScenario SCENARIO =
      new VectorDimension5TableScenario(keyspaceName, TABLE_NAME);

  @BeforeAll
  public final void createScenario() {
    SCENARIO.create();
  }

  @AfterAll
  public final void dropScenario() {
    SCENARIO.drop();
  }

  private static Stream<Arguments> findCommandNames() {

    var commands = new ArrayList<Arguments>();
    commands.add(Arguments.of(Command.CommandName.FIND));
    commands.add(Arguments.of(Command.CommandName.FIND_ONE));
    return commands.stream();
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findUnindexedVector(Command.CommandName commandName) {

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.UNINDEXED_VECTOR_COL),
            SCENARIO.columnValue(VectorDimension5TableScenario.UNINDEXED_VECTOR_COL));

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, null, null, sort)
        .hasSingleApiError(
            SortException.Code.CANNOT_VECTOR_SORT_NON_INDEXED_VECTOR_COLUMNS,
            SortException.class,
            "And has indexes on the vector columns: %s.\nThe command attempted to sort vector columns: %s"
                .formatted(SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
                    SCENARIO.fieldName(VectorDimension5TableScenario.UNINDEXED_VECTOR_COL)));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findMoreThanOneVector(Command.CommandName commandName) {

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            SCENARIO.columnValue(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            SCENARIO.fieldName(VectorDimension5TableScenario.UNINDEXED_VECTOR_COL),
            SCENARIO.columnValue(VectorDimension5TableScenario.UNINDEXED_VECTOR_COL));

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, null, null, sort)
        .hasSingleApiError(
            SortException.Code.MORE_THAN_ONE_VECTOR_SORT,
            SortException.class,
            "The command attempted to sort on the columns: %s, %s."
                .formatted(
                    SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
                    SCENARIO.fieldName(VectorDimension5TableScenario.UNINDEXED_VECTOR_COL)));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findUnknownVectorColumn(Command.CommandName commandName) {

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL) + "unknown",
            SCENARIO.columnValue(VectorDimension5TableScenario.INDEXED_VECTOR_COL));

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, null, null, sort)
        .hasSingleApiError(
            SortException.Code.CANNOT_SORT_UNKNOWN_COLUMNS,
            SortException.class,
            "The command attempted to sort the unknown columns: %s."
                .formatted(
                    SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL)
                        + "unknown"));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findNonVectorCol(Command.CommandName commandName) {

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.CONTENT_COL),
            SCENARIO.columnValue(VectorDimension5TableScenario.INDEXED_VECTOR_COL));

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, null, null, sort)
        .hasSingleApiError(
            SortException.Code.CANNOT_VECTOR_SORT_NON_VECTOR_COLUMNS,
            SortException.class,
            "The command attempted to sort the non vector columns: %s."
                .formatted(SCENARIO.fieldName(VectorDimension5TableScenario.CONTENT_COL)));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findSortVectorAndNon(Command.CommandName commandName) {

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            SCENARIO.columnValue(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            SCENARIO.fieldName(VectorDimension5TableScenario.CONTENT_COL),
            1); // asc sort

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, null, null, sort)
        .hasSingleApiError(
            SortException.Code.CANNOT_MIX_VECTOR_AND_NON_VECTOR_SORT,
            SortException.class,
            "The command attempted to sort the vector columns: %s.\nThe command attempted to sort the non-vector columns: %s."
                .formatted(
                    SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
                    SCENARIO.fieldName(VectorDimension5TableScenario.CONTENT_COL)));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findRandomVector(Command.CommandName commandName) {
    // Doing a sort for a vector we do not know if is in the table

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            SCENARIO.columnValue(VectorDimension5TableScenario.INDEXED_VECTOR_COL));

    var limit = 3;
    Map<String, Object> options =
        commandName == Command.CommandName.FIND ? ImmutableMap.of("limit", limit) : null;

    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(commandName, null, null, sort, options)
            .wasSuccessful();

    if (commandName == Command.CommandName.FIND) {
      validator.hasDocuments(limit);
    } else {
      validator.hasSingleDocument();
    }
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findKnownVector(Command.CommandName commandName) {
    // Doing a sort for a vector we know the vector is in the table, we can match on the expected
    // doc

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            (Object) VectorDimension5TableScenario.KNOWN_VECTOR);

    var limit = 1;
    Map<String, Object> options =
        commandName == Command.CommandName.FIND ? ImmutableMap.of("limit", limit) : null;

    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(commandName, null, null, sort, options)
            .wasSuccessful();

    if (commandName == Command.CommandName.FIND) {
      validator
          .hasDocuments(limit)
          .hasDocumentInPosition(0, VectorDimension5TableScenario.KNOWN_VECTOR_ROW_JSON);

    } else {
      validator.hasSingleDocument(VectorDimension5TableScenario.KNOWN_VECTOR_ROW_JSON);
    }
  }
}
