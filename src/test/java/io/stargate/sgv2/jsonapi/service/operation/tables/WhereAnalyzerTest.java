package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the {@link WhereCQLClauseAnalyzer} Testing the paths for select, delete one, delete
 * many, and update one.
 */
public class WhereAnalyzerTest {

  private static final TestData TEST_DATA = new TestData();

  private TestDataNames names() {
    return TEST_DATA.names;
  }

  // ==================================================================================================================
  // EASY CASES
  // (there are a number of combinations to test, pls keep organised into sections for easier
  // reading)
  // ==================================================================================================================

  private static Stream<Arguments> emptyFilterTests() {
    return Stream.of(
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.SELECT,
            WarningException.Code.ZERO_FILTER_OPERATIONS,
            null),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            null,
            FilterException.Code.MISSING_FILTER_FOR_UPDATE_DELETE),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_MANY,
            null,
            FilterException.Code.MISSING_FILTER_FOR_UPDATE_DELETE),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.UPDATE_ONE,
            null,
            FilterException.Code.MISSING_FILTER_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("emptyFilterTests")
  public void emptyFilter(
      WhereCQLClauseAnalyzer.StatementType statementType,
      WarningException.Code expectedWarningCode,
      FilterException.Code expectedFilterCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "emptyFilter() for %s".formatted(statementType), statementType);

    if (statementType == WhereCQLClauseAnalyzer.StatementType.SELECT) {
      fixture.analyze().assertAllowFilteringEnabled().assertOneWarning(expectedWarningCode);
    } else {
      fixture.analyzeMaybeFilterError(expectedFilterCode);
    }
  }

  private static Stream<Arguments> eqAllPrimaryKeysTests() {
    return Stream.of(
        Arguments.of(WhereCQLClauseAnalyzer.StatementType.SELECT),
        Arguments.of(WhereCQLClauseAnalyzer.StatementType.DELETE_ONE),
        Arguments.of(WhereCQLClauseAnalyzer.StatementType.DELETE_MANY),
        Arguments.of(WhereCQLClauseAnalyzer.StatementType.UPDATE_ONE));
  }

  @ParameterizedTest
  @MethodSource("eqAllPrimaryKeysTests")
  public void eqAllPrimaryKeys(WhereCQLClauseAnalyzer.StatementType statementType) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "eqAllPrimaryKeys() for %s".formatted(statementType), statementType);
    fixture.expression().eqAllPrimaryKeys().analyze().assertNoFilteringNoWarnings();
  }

  // ==================================================================================================================
  // NON PK COLUMNS - INDEXED AND UNINDEXED
  // ==================================================================================================================
  private static Stream<Arguments> eqOnNonPrimaryKeyOrIndexedTests() {
    return Stream.of(
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.SELECT, WarningException.Code.MISSING_INDEX, null),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            null,
            FilterException.Code.UNSUPPORTED_NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_MANY,
            null,
            FilterException.Code.UNSUPPORTED_NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.UPDATE_ONE,
            null,
            FilterException.Code.UNSUPPORTED_NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("eqOnNonPrimaryKeyOrIndexedTests")
  public void eqOnNonPrimaryKeyOrIndexed(
      WhereCQLClauseAnalyzer.StatementType statementType,
      WarningException.Code expectedWarningCode,
      FilterException.Code expectedFilterCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "eqOnNonPrimaryKeyOrIndexed() for %s".formatted(statementType), statementType)
            .expression()
            .eqOn(names().COL_REGULAR_1);

    if (statementType == WhereCQLClauseAnalyzer.StatementType.SELECT) {
      fixture
          .analyze()
          .assertAllowFilteringEnabled()
          .assertOneWarning(expectedWarningCode)
          .assertWarnOnUnindexedColumns(names().COL_REGULAR_1);
    } else {
      fixture.analyzeMaybeFilterError(expectedFilterCode);
    }
  }

  @ParameterizedTest
  @MethodSource("eqOnNonPrimaryKeyOrIndexedTests") // deliberately reusing the same test data
  public void eqOnIndexedAndRegular(
      WhereCQLClauseAnalyzer.StatementType statementType,
      WarningException.Code expectedWarningCode,
      FilterException.Code expectedFilterCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "eqOnIndexed() for %s".formatted(statementType), statementType)
            .expression()
            .eqOn(names().COL_INDEXED_1)
            .expression()
            .eqOn(names().COL_INDEXED_1)
            .expression()
            .eqOn(names().COL_REGULAR_1);

    if (statementType == WhereCQLClauseAnalyzer.StatementType.SELECT) {
      fixture
          .analyze()
          .assertAllowFilteringEnabled()
          .assertOneWarning(expectedWarningCode)
          .assertWarnOnUnindexedColumns(names().COL_REGULAR_1);
    } else {
      fixture.analyzeMaybeFilterError(expectedFilterCode);
    }
  }

  private static Stream<Arguments> eqOnIndexedTests() {
    return Stream.of(
        Arguments.of(WhereCQLClauseAnalyzer.StatementType.SELECT, null),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            FilterException.Code.UNSUPPORTED_NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_MANY,
            FilterException.Code.UNSUPPORTED_NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.UPDATE_ONE,
            FilterException.Code.UNSUPPORTED_NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("eqOnIndexedTests")
  public void eqOnIndexed(
      WhereCQLClauseAnalyzer.StatementType statementType, FilterException.Code expectedCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "eqOnIndexed() for %s".formatted(statementType), statementType)
            .expression()
            .eqOn(names().COL_INDEXED_1);

    if (statementType == WhereCQLClauseAnalyzer.StatementType.SELECT) {
      fixture.analyze().assertNoFilteringNoWarnings();
    } else {
      fixture.analyzeMaybeFilterError(expectedCode);
    }
  }
}
