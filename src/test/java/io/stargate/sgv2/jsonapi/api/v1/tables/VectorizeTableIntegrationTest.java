package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider.SAMPLE_VECTORIZE_CONTENTS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.util.TemplateRunner;
import io.stargate.sgv2.jsonapi.api.v1.util.scenarios.VectorizeTableScenario;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class VectorizeTableIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final String TABLE_NAME = "tablesVectorize";
  private static final VectorizeTableScenario SCENARIO =
      new VectorizeTableScenario(keyspaceName, TABLE_NAME);
  private static final String SAMPLE_VECTORIZE_CONTENT = SAMPLE_VECTORIZE_CONTENTS.getFirst();
  private ObjectMapper objectMapper = new ObjectMapper();

  @BeforeAll
  public final void createScenario() {
    SCENARIO.create();
  }

  @AfterAll
  public final void dropScenario() {
    SCENARIO.drop();
  }

  private static Stream<Arguments> insertCommandNames() {
    var commands = new ArrayList<Arguments>();
    // insertOne single vectorize in one doc
    commands.add(
        Arguments.of(
            CommandName.INSERT_ONE,
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1),
                SAMPLE_VECTORIZE_CONTENT,
                "id",
                String.valueOf(System.currentTimeMillis()))));
    // insertOne multiple vectorize in one doc
    commands.add(
        Arguments.of(
            CommandName.INSERT_ONE,
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1),
                SAMPLE_VECTORIZE_CONTENT,
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_2),
                SAMPLE_VECTORIZE_CONTENT,
                "id",
                String.valueOf(System.currentTimeMillis()))));

    // insertMany single vectorize in one doc
    commands.add(
        Arguments.of(
            CommandName.INSERT_MANY,
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1),
                SAMPLE_VECTORIZE_CONTENT,
                "id",
                String.valueOf(System.currentTimeMillis()))));
    // insertMany multiple vectorize in one doc
    commands.add(
        Arguments.of(
            CommandName.INSERT_MANY,
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1),
                SAMPLE_VECTORIZE_CONTENT,
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_2),
                SAMPLE_VECTORIZE_CONTENT,
                "id",
                String.valueOf(System.currentTimeMillis()))));

    return commands.stream();
  }

  @ParameterizedTest
  @MethodSource("insertCommandNames")
  public void successVectorizeInsert(CommandName commandName, ImmutableMap<String, Object> document)
      throws JsonProcessingException {

    var json = objectMapper.writeValueAsString(document);

    if (commandName.equals(CommandName.INSERT_ONE)) {
      assertTableCommand(keyspaceName, TABLE_NAME).templated().insertOne(json).wasSuccessful();
    } else {
      assertTableCommand(keyspaceName, TABLE_NAME).templated().insertMany(json).wasSuccessful();
    }
  }

  @ParameterizedTest
  @MethodSource("insertCommandNames")
  public void vectorColNoVectorizeDef(CommandName commandName) throws JsonProcessingException {

    Map<String, Object> document =
        Map.of(
            VectorizeTableScenario.fieldName(
                VectorizeTableScenario.INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF_1),
            SAMPLE_VECTORIZE_CONTENT,
            "id",
            String.valueOf(System.currentTimeMillis()));
    var json = objectMapper.writeValueAsString(document);
    if (commandName.equals(CommandName.INSERT_ONE)) {
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .insertOne(json)
          .hasSingleApiError(
              DocumentException.Code.INVALID_VECTORIZE_ON_COLUMN_WITHOUT_VECTORIZE_DEFINITION,
              DocumentException.class);
    } else {
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .insertMany(json)
          .hasSingleApiError(
              DocumentException.Code.INVALID_VECTORIZE_ON_COLUMN_WITHOUT_VECTORIZE_DEFINITION,
              DocumentException.class);
    }
  }

  private static Stream<Arguments> INVALID_VECTORIZE_SORT() {
    return Stream.of(
        Arguments.of(
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF_1),
                SAMPLE_VECTORIZE_CONTENT),
            SortException.Code.VECTORIZE_SORT_ON_VECTOR_COLUMN_WITHOUT_VECTORIZE_DEFINITION),
        Arguments.of(
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1),
                SAMPLE_VECTORIZE_CONTENT,
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF_1),
                SAMPLE_VECTORIZE_CONTENT),
            SortException.Code.MORE_THAN_ONE_VECTORIZE_SORT),
        Arguments.of(
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(VectorizeTableScenario.CONTENT_COL),
                SAMPLE_VECTORIZE_CONTENT),
            SortException.Code.VECTORIZE_SORT_ON_NON_VECTOR_COLUMN),
        Arguments.of(
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1),
                SAMPLE_VECTORIZE_CONTENT,
                VectorizeTableScenario.fieldName(VectorizeTableScenario.CONTENT_COL),
                1),
            SortException.Code.CANNOT_MIX_VECTOR_AND_NON_VECTOR_SORT),
        Arguments.of(
            ImmutableMap.of("no_column_exist", SAMPLE_VECTORIZE_CONTENT),
            SortException.Code.CANNOT_SORT_UNKNOWN_COLUMNS));
  }

  @ParameterizedTest
  @MethodSource("INVALID_VECTORIZE_SORT")
  public void invalidVectorizeSort(
      ImmutableMap<String, Object> sort, SortException.Code sortExceptionCode) {

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(CommandName.FIND_ONE, null, null, sort, null)
        .hasSingleApiError(sortExceptionCode, SortException.class);
  }

  private static Stream<Arguments> findCommandNames() {
    var commands = new ArrayList<Arguments>();
    commands.add(Arguments.of(CommandName.FIND));
    commands.add(Arguments.of(CommandName.FIND_ONE));
    return commands.stream();
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void successVectorizeSort(CommandName commandName) {
    ImmutableMap<String, Object> sort =
        ImmutableMap.of(
            VectorizeTableScenario.fieldName(
                VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1),
            SAMPLE_VECTORIZE_CONTENT);
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

  private static Stream<Arguments> VECTORIZE_UPDATE_SET() {
    return Stream.of(
        // no column exist to update set
        Arguments.of(
            ImmutableMap.of("no_column_exist", SAMPLE_VECTORIZE_CONTENT),
            UpdateException.Code.UNKNOWN_TABLE_COLUMNS),
        // successful vectorize update set
        Arguments.of(
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1),
                SAMPLE_VECTORIZE_CONTENT),
            null),
        //  successful multiple vectorize update set
        Arguments.of(
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1),
                SAMPLE_VECTORIZE_CONTENT,
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_2),
                SAMPLE_VECTORIZE_CONTENT),
            null),
        Arguments.of(
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF_1),
                SAMPLE_VECTORIZE_CONTENT),
            UpdateException.Code.INVALID_VECTORIZE_ON_COLUMN_WITHOUT_VECTORIZE_DEFINITION));
  }

  @ParameterizedTest
  @MethodSource("VECTORIZE_UPDATE_SET")
  public void vectorizeUpdateSet(
      ImmutableMap<String, Object> set, UpdateException.Code updateExceptionCode)
      throws JsonProcessingException {

    // InsertOne
    var inserterRowId = "rowId" + System.currentTimeMillis();
    insertOneRowWithRandowVector(inserterRowId);
    var changedVector =
        CustomITEmbeddingProvider.TEST_DATA_DIMENSION_5.get(SAMPLE_VECTORIZE_CONTENT);

    // UpdateOne
    ImmutableMap<String, Object> filterOnRow = ImmutableMap.of("id", inserterRowId);
    ImmutableMap<String, Object> updateSet = ImmutableMap.of("$set", set);
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .updateOne(filterOnRow, updateSet)
        .mayHaveSingleApiError(updateExceptionCode, UpdateException.class);

    // if no error, then vectors in set should all be updated by vectorize
    final Map<String, Object> vectorDataFields =
        set.keySet().stream().collect(Collectors.toMap(Function.identity(), a -> changedVector));
    if (updateExceptionCode == null) {
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .findOne(filterOnRow, null)
          .wasSuccessful()
          .hasDocumentFields(vectorDataFields);
    }

    // DeleteOne
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .delete(CommandName.DELETE_ONE, TemplateRunner.asJSON(filterOnRow))
        .wasSuccessful();
  }

  private void insertOneRowWithRandowVector(String rowId) throws JsonProcessingException {

    Random RANDOM = new Random();
    // sample:  [0.0, -0.5, 3.125, 0.1, 0.3]
    var randomVector =
        Stream.generate(RANDOM::nextFloat) // generate random floats
            .limit(5)
            .toList();

    Map<String, Object> document =
        Map.of(
            VectorizeTableScenario.fieldName(
                VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1),
            randomVector,
            VectorizeTableScenario.fieldName(
                VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_2),
            randomVector,
            VectorizeTableScenario.fieldName(
                VectorizeTableScenario.INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF_1),
            randomVector,
            VectorizeTableScenario.fieldName(VectorizeTableScenario.UNINDEXED_VECTOR_COL_1),
            randomVector,
            "id",
            rowId);
    var json = objectMapper.writeValueAsString(document);
    assertTableCommand(keyspaceName, TABLE_NAME).templated().insertOne(json).wasSuccessful();
  }
}
