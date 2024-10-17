package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import io.stargate.sgv2.jsonapi.service.operation.tables.WhereCQLClauseAnalyzer.StatementType;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the {@link WhereCQLClauseAnalyzer} Testing the paths for delete one, delete many, and
 * update
 *
 * <p>TODO: will create a ticket to merge this as the {@link SelectWhereAnalyzerTest} so we have one
 * definition of the combinations we run
 */
public class DeleteUpdateWhereAnalyzerTest {

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
            StatementType.DELETE_ONE, FilterException.Code.FILTER_REQUIRED_FOR_UPDATE_DELETE),
        Arguments.of(
            StatementType.DELETE_MANY, FilterException.Code.FILTER_REQUIRED_FOR_UPDATE_DELETE),
        Arguments.of(StatementType.UPDATE, FilterException.Code.FILTER_REQUIRED_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("emptyFilterTests")
  public void emptyFilter(StatementType statementType, FilterException.Code expectedCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "emptyFilter() for %s".formatted(statementType), statementType);
    fixture.analyzeMaybeFilterError(expectedCode);
  }

  private static Stream<Arguments> eqAllPrimaryKeysTests() {
    return Stream.of(
        Arguments.of(StatementType.DELETE_ONE, null),
        Arguments.of(StatementType.DELETE_MANY, null),
        Arguments.of(StatementType.UPDATE, null));
  }

  @ParameterizedTest
  @MethodSource("eqAllPrimaryKeysTests")
  public void eqAllPrimaryKeys(StatementType statementType, FilterException.Code expectedCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "eqAllPrimaryKeys() for %s".formatted(statementType), statementType);
    fixture.expression().eqAllPrimaryKeys().analyzeMaybeFilterError(expectedCode);
  }

  // ==================================================================================================================
  // NON PK COLUMNS - INDEXED AND UNINDEXED
  // ==================================================================================================================

  private static Stream<Arguments> eqOnNonPrimaryKeyOrIndexedTests() {
    return Stream.of(
        Arguments.of(
            StatementType.DELETE_ONE,
            FilterException.Code.NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE),
        Arguments.of(
            StatementType.DELETE_MANY,
            FilterException.Code.NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE),
        Arguments.of(
            StatementType.UPDATE, FilterException.Code.NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("eqOnNonPrimaryKeyOrIndexedTests")
  public void eqOnNonPrimaryKeyOrIndexed(
      StatementType statementType, FilterException.Code expectedCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "eqOnNonPrimaryKeyOrIndexed() for %s".formatted(statementType), statementType);
    fixture.expression().eqFirstNonPKOrIndexed().analyzeMaybeFilterError(expectedCode);
  }

  @ParameterizedTest
  @MethodSource("eqOnNonPrimaryKeyOrIndexedTests") // deliberately reusing the same test data
  public void eqOnIndexed(StatementType statementType, FilterException.Code expectedCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "eqOnIndexed() for %s".formatted(statementType), statementType);
    fixture.expression().eqOn(names().COL_INDEXED_1).analyzeMaybeFilterError(expectedCode);
  }

  // ==================================================================================================================
  // PARTITION KEY - PARTIAL PARTITION KEY
  // ==================================================================================================================

  private static Stream<Arguments> eqMissingPartitionKeyTests() {
    return Stream.of(
        Arguments.of(
            StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE),
        Arguments.of(StatementType.DELETE_MANY, FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER),
        Arguments.of(
            StatementType.UPDATE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("eqMissingPartitionKeyTests")
  public void eqMissingPartitionKey(
      StatementType statementType, FilterException.Code expectedCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("eqMissingPartitionKey()", statementType);

    fixture.expression().eqSkipOnePartitionKeys(0).analyzeMaybeFilterError(expectedCode);
  }

  // ==================================================================================================================
  // CLUSTERING COLUMNS - FULL PARTITION KEY, PARTIAL CLUSTERING KEY
  // ==================================================================================================================

  private static Stream<Arguments> skip1of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE),
        Arguments.of(StatementType.DELETE_MANY, FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER),
        Arguments.of(
            StatementType.UPDATE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("skip1of3ClusteringKeyTests")
  public void skip1of3ClusteringKey(
      StatementType statementType, FilterException.Code expectedCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("skip1of3ClusteringKey()", statementType);
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqSkipOneClusteringKeys(0)
        .analyzeMaybeFilterError(expectedCode);
  }

  private static Stream<Arguments> skip2of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE),
        Arguments.of(StatementType.DELETE_MANY, FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER),
        Arguments.of(
            StatementType.UPDATE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("skip2of3ClusteringKeyTests")
  public void skip2of3ClusteringKey(
      StatementType statementType, FilterException.Code expectedCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("skip2of3ClusteringKey()", statementType);
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqSkipOneClusteringKeys(1)
        .analyzeMaybeFilterError(expectedCode);
  }

  private static Stream<Arguments> skip3of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE),
        Arguments.of(StatementType.DELETE_MANY, null),
        Arguments.of(
            StatementType.UPDATE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("skip3of3ClusteringKeyTests")
  public void skip3of3ClusteringKey(
      StatementType statementType, FilterException.Code expectedCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("skip3of3ClusteringKey()", statementType);
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqSkipOneClusteringKeys(2)
        .analyzeMaybeFilterError(expectedCode);
  }

  private static Stream<Arguments> skip1and2of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE),
        Arguments.of(StatementType.DELETE_MANY, FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER),
        Arguments.of(
            StatementType.UPDATE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("skip1and2of3ClusteringKeyTests")
  public void skip1and2of3ClusteringKey(
      StatementType statementType, FilterException.Code expectedCode) {

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("skip1and2of3ClusteringKey()", statementType);
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqOnlyOneClusteringKey(2)
        .analyzeMaybeFilterError(expectedCode);
  }

  private static Stream<Arguments> skip2and3of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE),
        Arguments.of(StatementType.DELETE_MANY, null),
        Arguments.of(
            StatementType.UPDATE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("skip2and3of3ClusteringKeyTests")
  public void skip2and3of3ClusteringKey(
      StatementType statementType, FilterException.Code expectedCode) {

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("skip2and3of3ClusteringKey()", statementType);
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqOnlyOneClusteringKey(0)
        .analyzeMaybeFilterError(expectedCode);
  }

  private static Stream<Arguments> skip1and3of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE),
        Arguments.of(StatementType.DELETE_MANY, FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER),
        Arguments.of(
            StatementType.UPDATE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE));
  }

  @ParameterizedTest
  @MethodSource("skip1and3of3ClusteringKeyTests")
  public void skip1and3of3ClusteringKey(
      StatementType statementType, FilterException.Code expectedCode) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("skip1and3of3ClusteringKey()", statementType);
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqOnlyOneClusteringKey(1)
        .analyzeMaybeFilterError(expectedCode);
  }
}
