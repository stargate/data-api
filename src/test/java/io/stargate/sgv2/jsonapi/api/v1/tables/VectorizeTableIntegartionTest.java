package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider.SAMPLE_VECTORIZE_CONTENTS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.v1.util.scenarios.VectorizeTableScenario;
import io.stargate.sgv2.jsonapi.exception.*;
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
public class VectorizeTableIntegartionTest extends AbstractTableIntegrationTestBase {

  private static final String TABLE_NAME = "vectorize" + System.currentTimeMillis();
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
    commands.add(Arguments.of(Command.CommandName.INSERT_ONE));
    commands.add(Arguments.of(Command.CommandName.INSERT_MANY));
    return commands.stream();
  }

  @ParameterizedTest
  @MethodSource("insertCommandNames")
  public void successVectorizeInsert(Command.CommandName commandName)
      throws JsonProcessingException {

    Map<String, Object> document =
        Map.of(
            VectorizeTableScenario.fieldName(
                VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF),
            SAMPLE_VECTORIZE_CONTENT,
            "id",
            String.valueOf(System.currentTimeMillis()));
    var json = objectMapper.writeValueAsString(document);
    if (commandName.equals(Command.CommandName.INSERT_ONE)) {
      assertTableCommand(keyspaceName, TABLE_NAME).templated().insertOne(json).wasSuccessful();
    } else {
      assertTableCommand(keyspaceName, TABLE_NAME).templated().insertMany(json).wasSuccessful();
    }
  }

  @ParameterizedTest
  @MethodSource("insertCommandNames")
  public void vectorColNoVectorizeDef(Command.CommandName commandName)
      throws JsonProcessingException {

    Map<String, Object> document =
        Map.of(
            VectorizeTableScenario.fieldName(
                VectorizeTableScenario.INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF),
            SAMPLE_VECTORIZE_CONTENT,
            "id",
            String.valueOf(System.currentTimeMillis()));
    var json = objectMapper.writeValueAsString(document);
    if (commandName.equals(Command.CommandName.INSERT_ONE)) {
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
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF),
                SAMPLE_VECTORIZE_CONTENT),
            SortException.Code.VECTORIZE_SORT_ON_VECTOR_COLUMN_WITHOUT_VECTORIZE_DEFINITION),
        Arguments.of(
            ImmutableMap.of(
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF),
                SAMPLE_VECTORIZE_CONTENT,
                VectorizeTableScenario.fieldName(
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF),
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
                    VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF),
                SAMPLE_VECTORIZE_CONTENT,
                VectorizeTableScenario.fieldName(VectorizeTableScenario.CONTENT_COL),
                1),
            SortException.Code.CANNOT_MIX_VECTOR_AND_NON_VECTOR_SORT));
  }

  @ParameterizedTest
  @MethodSource("INVALID_VECTORIZE_SORT")
  public void invalidVectorizeSort(
      ImmutableMap<String, Object> sort, SortException.Code sortExceptionCode) {

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(Command.CommandName.FIND_ONE, null, null, sort, null)
        .hasSingleApiError(sortExceptionCode, SortException.class);
  }

  private static Stream<Arguments> findCommandNames() {
    var commands = new ArrayList<Arguments>();
    commands.add(Arguments.of(Command.CommandName.FIND));
    commands.add(Arguments.of(Command.CommandName.FIND_ONE));
    return commands.stream();
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void successVectorizeSort(Command.CommandName commandName) {
    ImmutableMap<String, Object> sort =
        ImmutableMap.of(
            VectorizeTableScenario.fieldName(
                VectorizeTableScenario.INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF),
            SAMPLE_VECTORIZE_CONTENT);
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
}
