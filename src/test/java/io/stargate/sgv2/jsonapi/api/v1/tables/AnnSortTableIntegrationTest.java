package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.util.scenarios.VectorDimension5TableScenario;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
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
    return Arrays.asList(Arguments.of(CommandName.FIND), Arguments.of(CommandName.FIND_ONE))
        .stream();
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findUnindexedVector(CommandName commandName) {

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
                .formatted(
                    SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
                    SCENARIO.fieldName(VectorDimension5TableScenario.UNINDEXED_VECTOR_COL)));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findMoreThanOneVector(CommandName commandName) {

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
            SortException.Code.CANNOT_SORT_ON_SPECIAL_WITH_OTHERS,
            SortException.class,
            "The command used a sort clause with a special (lexical/vector/vectorize) sort",
            "command attempted to use vector/vectorize sort on columns: %s, %s"
                .formatted(
                    SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
                    SCENARIO.fieldName(VectorDimension5TableScenario.UNINDEXED_VECTOR_COL)));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findUnknownVectorColumn(CommandName commandName) {

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
  public void findNonVectorCol(CommandName commandName) {

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
            "The command attempted to sort the non-vector columns: %s."
                .formatted(SCENARIO.fieldName(VectorDimension5TableScenario.CONTENT_COL)));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findSortVectorAndNon(CommandName commandName) {

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
            SortException.Code.CANNOT_SORT_ON_SPECIAL_WITH_OTHERS,
            SortException.class,
            "The command used a sort clause with a special (lexical/vector/vectorize) sort combined",
            "The command attempted to use vector/vectorize sort on columns: %s"
                .formatted(SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL)),
            "The command attempted to use regular sort on columns: %s"
                .formatted(SCENARIO.fieldName(VectorDimension5TableScenario.CONTENT_COL)));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findRandomVector(CommandName commandName) {
    // Doing a sort for a vector we do not know if is in the table

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            SCENARIO.columnValue(VectorDimension5TableScenario.INDEXED_VECTOR_COL));

    var limit = 3;
    Map<String, Object> options =
        commandName == CommandName.FIND ? ImmutableMap.of("limit", limit) : null;

    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(commandName, null, null, sort, options)
            .wasSuccessful();

    if (commandName == CommandName.FIND) {
      validator.hasDocuments(limit);
    } else {
      validator.hasSingleDocument();
    }
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findKnownVector(CommandName commandName) {
    // Doing a sort for a vector we know the vector is in the table, we can match on the expected
    // doc

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            (Object) VectorDimension5TableScenario.KNOWN_VECTOR);

    var limit = 1;
    Map<String, Object> options =
        commandName == CommandName.FIND ? ImmutableMap.of("limit", limit) : null;

    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(commandName, null, null, sort, options)
            .wasSuccessful();

    if (commandName == CommandName.FIND) {
      validator
          .hasDocuments(limit)
          .hasDocumentInPosition(0, VectorDimension5TableScenario.KNOWN_VECTOR_ROW_JSON);

    } else {
      validator.hasSingleDocument(VectorDimension5TableScenario.KNOWN_VECTOR_ROW_JSON);
    }
  }

  @Test
  public void findSortVectorExceedLimit() {

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            SCENARIO.columnValue(VectorDimension5TableScenario.INDEXED_VECTOR_COL));

    var limit = 1001;
    Map<String, Object> options = ImmutableMap.of("limit", limit);

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(null, null, sort, options)
        .hasSingleApiError(
            SortException.Code.CANNOT_VECTOR_SORT_WITH_LIMIT_EXCEEDS_MAX,
            SortException.class,
            "Vector sorting is limited to a maximum of 1000 rows.",
            "The command attempted to sort the vector column: %s with a limit of %s."
                .formatted(
                    SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL), limit));
  }
}
