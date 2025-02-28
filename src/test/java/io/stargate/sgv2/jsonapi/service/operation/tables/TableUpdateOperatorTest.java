package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdatePullAllResolver;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdatePushResolver;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdateSetResolver;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdateUnsetResolver;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TableUpdateOperatorTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private static final TestData TEST_DATA = new TestData();

  private static TestDataNames names() {
    return TEST_DATA.names;
  }

  // ==================================================================================================================
  // $set
  static Stream<Arguments> setOperatorOnColumns() {
    return Stream.of(
        // map,set,list
        Arguments.of(
            "{\"%s\": {\"key1\": \"value1\"}}".formatted(names().CQL_MAP_COLUMN),
            new JsonLiteral(
                Map.of("key1", new JsonLiteral("value1", JsonType.STRING)), JsonType.SUB_DOC)),
        Arguments.of(
            "{\"%s\": [[\"key1\", \"value1\"]]}".formatted(names().CQL_MAP_COLUMN),
            new JsonLiteral(
                Map.of(
                    new JsonLiteral("key1", JsonType.STRING),
                    new JsonLiteral("value1", JsonType.STRING)),
                JsonType.SUB_DOC)),
        Arguments.of(
            "{\"%s\": [\"value1\"]}".formatted(names().CQL_LIST_COLUMN),
            new JsonLiteral(List.of(new JsonLiteral("value1", JsonType.STRING)), JsonType.ARRAY)),
        Arguments.of(
            "{\"%s\": [\"value1\"]}".formatted(names().CQL_SET_COLUMN),
            new JsonLiteral(List.of(new JsonLiteral("value1", JsonType.STRING)), JsonType.ARRAY)),

        // primitive
        Arguments.of(
            "{\"%s\": \"abc\"}}".formatted(names().CQL_TEXT_COLUMN),
            new JsonLiteral("abc", JsonType.STRING),
            JsonType.SUB_DOC));
  }

  @ParameterizedTest
  @MethodSource("setOperatorOnColumns")
  public void setOperator(String json, JsonLiteral<?> expectedJsonLiteral) throws Exception {
    var fixture =
        TEST_DATA
            .tableUpdateOperator()
            .tableWithMapSetList(new TableUpdateSetResolver(), "test $set on columns");

    fixture
        .resolve(json)
        .assertSingleAssignment()
        .assertFirstAssignmentEqual(UpdateOperator.SET, expectedJsonLiteral);
  }

  // ==================================================================================================================
  // $unset
  static Stream<Arguments> unsetOperatorOnColumns() {
    return Stream.of(
        // map,set,list
        Arguments.of("{\"%s\": {\"key1\": \"value1\"}}".formatted(names().CQL_MAP_COLUMN)),
        Arguments.of("{\"%s\": [[\"key1\", \"value1\"]]}".formatted(names().CQL_MAP_COLUMN)),
        Arguments.of("{\"%s\": [\"value1\"]}".formatted(names().CQL_LIST_COLUMN)),
        Arguments.of("{\"%s\": [\"value1\"]}".formatted(names().CQL_SET_COLUMN)),
        // primitive
        Arguments.of(
            "{\"%s\": \"abc\"}}".formatted(names().CQL_TEXT_COLUMN),
            new JsonLiteral("abc", JsonType.STRING)));
  }

  @ParameterizedTest
  @MethodSource("unsetOperatorOnColumns")
  public void unsetOperator(String json) throws Exception {
    var fixture =
        TEST_DATA
            .tableUpdateOperator()
            .tableWithMapSetList(new TableUpdateUnsetResolver(), "test $unset on columns");

    fixture
        .resolve(json)
        .assertSingleAssignment()
        .assertFirstAssignmentEqual(UpdateOperator.UNSET, new JsonLiteral<>(null, JsonType.NULL));
  }

  // ==================================================================================================================
  // $push, $each
  static Stream<Arguments> pushOperatorOnColumns() {
    return Stream.of(
        // map, $push, object format
        Arguments.of(
            "{\"%s\": {\"key1\": \"value1\"}}".formatted(names().CQL_MAP_COLUMN),
            new JsonLiteral(
                Map.of(
                    new JsonLiteral("key1", JsonType.STRING),
                    new JsonLiteral("value1", JsonType.STRING)),
                JsonType.SUB_DOC),
            null,
            null),
        // map, $push single entry, tuple format
        Arguments.of(
            "{\"%s\": [\"key1\", \"value1\"]}".formatted(names().CQL_MAP_COLUMN),
            new JsonLiteral(
                Map.of(
                    new JsonLiteral("key1", JsonType.STRING),
                    new JsonLiteral("value1", JsonType.STRING)),
                JsonType.SUB_DOC),
            null,
            null),
        // map, $push, $each, tuple format
        Arguments.of(
            "{\"%s\": {\"$each\": [{\"key1\": \"value1\"}]}}".formatted(names().CQL_MAP_COLUMN),
            new JsonLiteral(
                Map.of(
                    new JsonLiteral("key1", JsonType.STRING),
                    new JsonLiteral("value1", JsonType.STRING)),
                JsonType.SUB_DOC),
            null,
            null),
        // map, $push, $each, tuple format
        Arguments.of(
            "{\"%s\": {\"$each\": [[\"key1\", \"value1\"]]}}".formatted(names().CQL_MAP_COLUMN),
            new JsonLiteral(
                Map.of(
                    new JsonLiteral("key1", JsonType.STRING),
                    new JsonLiteral("value1", JsonType.STRING)),
                JsonType.SUB_DOC),
            null,
            null),
        Arguments.of(
            "{\"%s\": {\"$each\": [\"abc\"]}}".formatted(names().CQL_LIST_COLUMN),
            new JsonLiteral(List.of(new JsonLiteral("abc", JsonType.STRING)), JsonType.ARRAY),
            null,
            null),
        Arguments.of(
            "{\"%s\": {\"$each\": [\"abc\"]}}".formatted(names().CQL_SET_COLUMN),
            new JsonLiteral(List.of(new JsonLiteral("abc", JsonType.STRING)), JsonType.ARRAY),
            null,
            null),
        // combine $push and $each to add multiple
        Arguments.of(
            "{\"%s\": {\"key1\": \"value1\", \"key2\": \"value2\"}}"
                .formatted(names().CQL_MAP_COLUMN),
            null,
            UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR,
            "combine $push and $each for adding multiple elements"),
        Arguments.of(
            "{\"%s\": [[\"key1\", \"value1\"], [\"key2\", \"value2\"]]}"
                .formatted(names().CQL_MAP_COLUMN),
            null,
            UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR,
            "combine $push and $each for adding multiple elements"),
        Arguments.of(
            "{\"%s\": [\"abc\"]}".formatted(names().CQL_SET_COLUMN),
            null,
            UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR,
            "combine $push and $each for adding multiple elements"),
        Arguments.of(
            "{\"%s\": [\"abc\"]}".formatted(names().CQL_LIST_COLUMN),
            null,
            UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR,
            "combine $push and $each for adding multiple elements"),
        // primitive
        Arguments.of(
            "{\"%s\": \"abc\"}}".formatted(names().CQL_TEXT_COLUMN),
            null,
            UpdateException.Code.UNSUPPORTED_UPDATE_OPERATORS_FOR_PRIMITIVE_COLUMNS,
            "$push and $pullAll are only supported against map, set, list columns."));
  }

  @ParameterizedTest
  @MethodSource("pushOperatorOnColumns")
  public void pushOperator(
      String json,
      JsonLiteral<?> expectedJsonLiteral,
      UpdateException.Code expectedErrorCode,
      String errorSnippet)
      throws Exception {
    var fixture =
        TEST_DATA
            .tableUpdateOperator()
            .tableWithMapSetList(new TableUpdatePushResolver(), "test $push, $each on columns");
    if (expectedErrorCode != null) {
      fixture.resolveWithError(json, UpdateException.class, expectedErrorCode, errorSnippet);
      return;
    }

    fixture
        .resolve(json)
        .assertSingleAssignment()
        .assertFirstAssignmentEqual(UpdateOperator.PUSH, expectedJsonLiteral);
  }

  // ==================================================================================================================
  // $pullAll
  static Stream<Arguments> pullAllOperatorOnColumns() {
    return Stream.of(
        // map,set,list
        Arguments.of(
            "{\"%s\": [\"key1\"]}".formatted(names().CQL_MAP_COLUMN),
            new JsonLiteral(List.of(new JsonLiteral("key1", JsonType.STRING)), JsonType.ARRAY),
            null,
            null),
        Arguments.of(
            "{\"%s\": [\"value1\"]}".formatted(names().CQL_LIST_COLUMN),
            new JsonLiteral(List.of(new JsonLiteral("value1", JsonType.STRING)), JsonType.ARRAY),
            null,
            null),
        Arguments.of(
            "{\"%s\": [\"value1\"]}".formatted(names().CQL_SET_COLUMN),
            new JsonLiteral(List.of(new JsonLiteral("value1", JsonType.STRING)), JsonType.ARRAY),
            null,
            null),
        Arguments.of(
            "{\"%s\": {\"key1\": \"value1\"}}".formatted(names().CQL_MAP_COLUMN),
            null,
            UpdateException.Code.UPDATE_OPERATOR_PULL_ALL_REQUIRES_ARRAY_VALUE,
            "Update operator $pullAll requires array value to remove elements from the map, set, list column"),
        Arguments.of(
            "{\"%s\": \"abc\"}".formatted(names().CQL_SET_COLUMN),
            null,
            UpdateException.Code.UPDATE_OPERATOR_PULL_ALL_REQUIRES_ARRAY_VALUE,
            "Update operator $pullAll requires array value to remove elements from the map, set, list column"),
        Arguments.of(
            "{\"%s\": 123}".formatted(names().CQL_LIST_COLUMN),
            null,
            UpdateException.Code.UPDATE_OPERATOR_PULL_ALL_REQUIRES_ARRAY_VALUE,
            "Update operator $pullAll requires array value to remove elements from the map, set, list column"),

        // primitive
        Arguments.of(
            "{\"%s\": \"abc\"}}".formatted(names().CQL_TEXT_COLUMN),
            null,
            UpdateException.Code.UNSUPPORTED_UPDATE_OPERATORS_FOR_PRIMITIVE_COLUMNS,
            "$push and $pullAll are only supported against map, set, list columns."));
  }

  @ParameterizedTest
  @MethodSource("pullAllOperatorOnColumns")
  public void pullAllOperator(
      String json,
      JsonLiteral<?> expectedJsonLiteral,
      UpdateException.Code expectedErrorCode,
      String errorSnippet)
      throws Exception {
    var fixture =
        TEST_DATA
            .tableUpdateOperator()
            .tableWithMapSetList(new TableUpdatePullAllResolver(), "test $pullAll on columns");
    if (expectedErrorCode != null) {
      fixture.resolveWithError(json, UpdateException.class, expectedErrorCode, errorSnippet);
      return;
    }

    fixture
        .resolve(json)
        .assertSingleAssignment()
        .assertFirstAssignmentEqual(UpdateOperator.PULL_ALL, expectedJsonLiteral);
  }
}
