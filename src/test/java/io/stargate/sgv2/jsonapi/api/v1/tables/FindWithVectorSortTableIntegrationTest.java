package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import com.fasterxml.jackson.databind.node.ArrayNode;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
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
public class FindWithVectorSortTableIntegrationTest extends AbstractTableIntegrationTestBase {

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
    commands.add(Arguments.of(CommandName.FIND));
    commands.add(Arguments.of(CommandName.FIND_ONE));
    return commands.stream();
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
            "The command attempted to use vector/vectorize sort on columns: %s, %s"
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
            "The command used a sort clause with a special (lexical/vector/vectorize) sort",
            "The command attempted to use vector/vectorize sort on columns: "
                + SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            "The command attempted to use regular sort on columns: "
                + SCENARIO.fieldName(VectorDimension5TableScenario.CONTENT_COL));
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
      validator.hasDocuments(limit).includeSimilarityScoreDocuments(false);
    } else {
      validator.hasSingleDocument().includeSimilarityScoreSingleDocument(false);
    }
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findSimilarityScore(CommandName commandName) {

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            SCENARIO.columnValue(VectorDimension5TableScenario.INDEXED_VECTOR_COL));

    var limit = 3;
    Map<String, Object> options =
        commandName == CommandName.FIND
            ? ImmutableMap.of("limit", limit, "includeSimilarity", true)
            : ImmutableMap.of("includeSimilarity", true);

    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(commandName, null, null, sort, options)
            .wasSuccessful();

    if (commandName == CommandName.FIND) {
      validator.hasDocuments(limit).includeSimilarityScoreDocuments(true).includeSortVector(false);
    } else {
      validator
          .hasSingleDocument()
          .includeSimilarityScoreSingleDocument(true)
          .includeSortVector(false);
    }
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findSimilarityScoreWithoutANN(CommandName commandName) {
    // Without Vector sort clause, similarityScore won't be included

    var limit = 3;
    Map<String, Object> options =
        commandName == CommandName.FIND
            ? ImmutableMap.of("limit", limit, "includeSimilarity", true)
            : ImmutableMap.of("includeSimilarity", true);

    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(commandName, null, null, null, options)
            .wasSuccessful();

    if (commandName == CommandName.FIND) {
      validator.hasDocuments(limit).includeSimilarityScoreDocuments(false).includeSortVector(false);
    } else {
      validator
          .hasSingleDocument()
          .includeSimilarityScoreSingleDocument(false)
          .includeSortVector(false);
    }
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findReturnVector(CommandName commandName) {

    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            SCENARIO.columnValue(VectorDimension5TableScenario.INDEXED_VECTOR_COL));

    var limit = 3;
    Map<String, Object> options =
        commandName == CommandName.FIND
            ? ImmutableMap.of("limit", limit, "includeSortVector", true)
            : ImmutableMap.of("includeSortVector", true);

    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(commandName, null, null, sort, options)
            .wasSuccessful();

    if (commandName == CommandName.FIND) {
      validator.hasDocuments(limit).includeSimilarityScoreDocuments(false).includeSortVector(true);
    } else {
      validator
          .hasSingleDocument()
          .includeSimilarityScoreSingleDocument(false)
          .includeSortVector(true);
    }
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void returnVectorWithoutANN(CommandName commandName) {
    // Without Vector sort clause, vector won't be included in status map.

    var limit = 3;
    Map<String, Object> options =
        commandName == CommandName.FIND
            ? ImmutableMap.of("limit", limit, "includeSortVector", true)
            : ImmutableMap.of("includeSortVector", true);

    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(commandName, null, null, null, options)
            .wasSuccessful();

    if (commandName == CommandName.FIND) {
      validator.hasDocuments(limit).includeSimilarityScoreDocuments(false).includeSortVector(false);
    } else {
      validator
          .hasSingleDocument()
          .includeSimilarityScoreSingleDocument(false)
          .includeSortVector(false);
    }
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findRandomBinaryVector(CommandName commandName) {
    // Doing a sort for a vector we do not know if is in the table

    ArrayNode rawData =
        (ArrayNode) SCENARIO.columnValue(VectorDimension5TableScenario.INDEXED_VECTOR_COL);
    float[] vectorData = new float[rawData.size()];
    for (int i = 0; i < vectorData.length; i++) {
      vectorData[i] = rawData.get(i).floatValue();
    }
    var vectorString = generateBase64EncodedBinaryVector(vectorData);
    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            (Object) Map.of("$binary", vectorString));

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
          .hasDocumentInPosition(0, VectorDimension5TableScenario.KNOWN_VECTOR_ROW_JSON)
          .includeSortVector(false)
          .includeSimilarityScoreDocuments(false);

    } else {
      validator
          .hasSingleDocument(VectorDimension5TableScenario.KNOWN_VECTOR_ROW_JSON)
          .includeSortVector(false)
          .includeSimilarityScoreSingleDocument(false);
    }
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void findKnownVectorAsBinary(CommandName commandName) {
    // Doing a sort for a vector we know the vector is in the table, we can match on the expected
    // doc
    var vectorString =
        generateBase64EncodedBinaryVector(VectorDimension5TableScenario.KNOWN_VECTOR_ARRAY);
    var sort =
        ImmutableMap.of(
            SCENARIO.fieldName(VectorDimension5TableScenario.INDEXED_VECTOR_COL),
            (Object) Map.of("$binary", vectorString));

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
}
