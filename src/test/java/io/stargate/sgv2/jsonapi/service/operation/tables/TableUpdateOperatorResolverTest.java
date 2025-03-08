package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import io.stargate.sgv2.jsonapi.service.resolver.update.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for the {@link TableUpdateOperatorResolver} and it's subclasses */
public class TableUpdateOperatorResolverTest {

  private static final TestData TEST_DATA = new TestData();

  private static TestDataNames names() {
    return TEST_DATA.names;
  }

  // ==================================================================================================================
  // $set

  static Stream<Arguments> setOperations() {
    return Stream.of(
        // map,set,list
        Arguments.of(
                """
          {"%s": {"key1": "value1"}}
          """
                .formatted(names().CQL_MAP_COLUMN),
            Map.of("key1", "value1")),
        Arguments.of(
                """
            {"%s": [["key1", "value1"]]}
            """
                .formatted(names().CQL_MAP_COLUMN),
            Map.of("key1", "value1")),
        Arguments.of(
                """
            {"%s": ["value1"]}
            """
                .formatted(names().CQL_LIST_COLUMN),
            List.of("value1")),
        Arguments.of(
                """
            {"%s": ["value1"]}
            """
                .formatted(names().CQL_SET_COLUMN),
            Set.of("value1")),
        // primitive
        Arguments.of(
                """
            {"%s": "abc"}
            """
                .formatted(names().CQL_TEXT_COLUMN),
            "abc"));
  }

  @ParameterizedTest
  @MethodSource("setOperations")
  public void setApiName(String setRhs, Object expectedCqlValue) throws Exception {
    var fixture =
        TEST_DATA
            .tableUpdateOperator()
            .tableWithMapSetList(new TableUpdateSetResolver(), "test $set on columns");

    fixture
        .resolve(setRhs)
        .assertSingleAssignment()
        .assertFirstAssignmentEqual(UpdateOperator.SET, expectedCqlValue);
  }

  // ==================================================================================================================
  // $unset
  static Stream<Arguments> unsetOperations() {
    return Stream.of(
        // map,set,list
        Arguments.of(
                """
            {"%s": {"key1": "value1"}}
            """
                .formatted(names().CQL_MAP_COLUMN)),
        Arguments.of(
                """
            {"%s": [["key1", "value1"]]}
            """
                .formatted(names().CQL_MAP_COLUMN)),
        Arguments.of(
                """
            {"%s": ["value1"]}
            """
                .formatted(names().CQL_LIST_COLUMN)),
        Arguments.of(
                """
            {"%s": ["value1"]}
            """
                .formatted(names().CQL_SET_COLUMN)),
        // primitive
        Arguments.of(
                """
            {"%s": "abc"}}
            """
                .formatted(names().CQL_TEXT_COLUMN),
            new JsonLiteral("abc", JsonType.STRING)));
  }

  @ParameterizedTest
  @MethodSource("unsetOperations")
  public void unsetApiName(String unsetRhs) throws Exception {
    var fixture =
        TEST_DATA
            .tableUpdateOperator()
            .tableWithMapSetList(new TableUpdateUnsetResolver(), "test $unset on columns");

    fixture
        .resolve(unsetRhs)
        .assertSingleAssignment()
        .assertFirstAssignmentEqual(UpdateOperator.UNSET, null);
  }

  // ==================================================================================================================
  // $push, $each
  static Stream<Arguments> pushOperations() {
    return Stream.of(
        // map, $push, object format
        Arguments.of(
                """
            {"%s": {"key1": "value1"}}
            """
                .formatted(names().CQL_MAP_COLUMN),
            Map.of("key1", "value1"),
            null,
            null),
        // map, $push single entry, tuple format
        Arguments.of(
                """
            {"%s": ["key1", "value1"]}
            """
                .formatted(names().CQL_MAP_COLUMN),
            Map.of("key1", "value1"),
            null,
            null),
        // map, $push, $each, tuple format
        Arguments.of(
                """
            {"%s": {"$each": [{"key1": "value1"}]}}
            """
                .formatted(names().CQL_MAP_COLUMN),
            Map.of("key1", "value1"),
            null,
            null),
        // map, $push, $each, tuple format
        Arguments.of(
                """
            {"%s": {"$each": [["key1", "value1"]]}}
            """
                .formatted(names().CQL_MAP_COLUMN),
            Map.of("key1", "value1"),
            null,
            null),
        Arguments.of(
                """
            {"%s": {"$each": ["abc"]}}
            """
                .formatted(names().CQL_LIST_COLUMN),
            List.of("abc"),
            null,
            null),
        Arguments.of(
                """
            {"%s": {"$each": ["abc"]}}
            """
                .formatted(names().CQL_SET_COLUMN),
            Set.of("abc"),
            null,
            null),
        // combine $push and $each to add multiple
        Arguments.of(
                """
            {"%s": {"key1": "value1", "key2": "value2"}}
            """
                .formatted(names().CQL_MAP_COLUMN),
            null,
            UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE,
            "combine $push and $each for adding multiple elements"),
        Arguments.of(
                """
            {"%s": [["key1", "value1"], ["key2", "value2"]]}
            """
                .formatted(names().CQL_MAP_COLUMN),
            null,
            UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE,
            "combine $push and $each for adding multiple elements"),
        Arguments.of(
                """
            {"%s": ["abc"]}
            """
                .formatted(names().CQL_SET_COLUMN),
            null,
            UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE,
            "combine $push and $each for adding multiple elements"),
        Arguments.of(
                """
            {"%s": ["abc"]}
            """
                .formatted(names().CQL_LIST_COLUMN),
            null,
            UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE,
            "combine $push and $each for adding multiple elements"),
        // primitive
        Arguments.of(
                """
            {"%s": "abc"}}
            """
                .formatted(names().CQL_TEXT_COLUMN),
            null,
            UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR,
            "$push and $pullAll are only supported against map, set, list columns."));
  }

  @ParameterizedTest
  @MethodSource("pushOperations")
  public void pushApiName(
      String json,
      Object expectedValue,
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
        .assertFirstAssignmentEqual(UpdateOperator.PUSH, expectedValue);
  }

  // ==================================================================================================================
  // $pullAll
  static Stream<Arguments> pullAllOperations() {
    return Stream.of(
        // map,set,list
        Arguments.of(
                """
            {"%s": ["key1"]}
            """
                .formatted(names().CQL_MAP_COLUMN),
            List.of("key1"),
            null,
            null),
        Arguments.of(
                """
            {"%s": ["value1"]}
            """
                .formatted(names().CQL_LIST_COLUMN),
            List.of("value1"),
            null,
            null),
        Arguments.of(
                """
            {"%s": ["value1"]}
            """
                .formatted(names().CQL_SET_COLUMN),
            Set.of("value1"),
            null,
            null),
        Arguments.of(
                """
            {"%s": {"key1": "value1"}}
            """
                .formatted(names().CQL_MAP_COLUMN),
            null,
            UpdateException.Code.INVALID_UPDATE_COLUMN_VALUES,
            "The update included invalid values for the columns: %s"
                .formatted(errFmt(names().CQL_MAP_COLUMN))),
        Arguments.of(
                """
            {"%s": "abc"}
            """
                .formatted(names().CQL_SET_COLUMN),
            null,
            UpdateException.Code.INVALID_UPDATE_COLUMN_VALUES,
            "The update included invalid values for the columns: %s"
                .formatted(errFmt(names().CQL_SET_COLUMN))),
        Arguments.of(
                """
            {"%s": 123}
            """
                .formatted(names().CQL_LIST_COLUMN),
            null,
            UpdateException.Code.INVALID_UPDATE_COLUMN_VALUES,
            "The update included invalid values for the columns: %s"
                .formatted(errFmt(names().CQL_LIST_COLUMN))),

        // primitive
        Arguments.of(
                """
            {"%s": "abc"}}
            """
                .formatted(names().CQL_TEXT_COLUMN),
            null,
            UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR,
            "$push and $pullAll are only supported against map, set, list columns."));
  }

  @ParameterizedTest
  @MethodSource("pullAllOperations")
  public void pullAllApiName(
      String json,
      Object expectedValue,
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
        .assertFirstAssignmentEqual(UpdateOperator.PULL_ALL, expectedValue);
  }
}
