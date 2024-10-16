package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import org.junit.jupiter.api.Test;

/** Tests for the {@link WhereCQLClauseAnalyzer}. Focus on Update Statement type */
public class UpdateWhereAnalyzerTest {

  private static final TestData TEST_DATA = new TestData();

  private TestDataNames names() {
    return TEST_DATA.names;
  }

  @Test
  public void emptyFilter() {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "update_emptyFilter()", WhereCQLClauseAnalyzer.StatementType.UPDATE);
    fixture
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(FilterException.Code.NO_FILTER_UPDATE_DELETE);
  }

  @Test
  public void eqAllPrimaryKeys() {

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "update_eqAllPrimaryKeys()", WhereCQLClauseAnalyzer.StatementType.UPDATE);
    fixture.expression().eqAllPrimaryKeys().analyze().assertNoFilteringNoWarnings();
  }

  @Test
  public void eqOnNonPrimaryKey() {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "update_eqOnNonPrimaryKey()", WhereCQLClauseAnalyzer.StatementType.UPDATE);
    fixture
        .expression()
        .eqFirstNonPKOrIndexed()
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(FilterException.Code.NON_PRIMARY_KEY_COLUMNS_USED_UPDATE_DELETE);
  }

  @Test
  public void eqSkippingClusteringKey() {

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "update_skippingClusteringKey()", WhereCQLClauseAnalyzer.StatementType.UPDATE);
    fixture
        .expression()
        .eqSkipOneClusteringKeys(0)
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(FilterException.Code.PRIMARY_KEY_NOT_FULLY_SPECIFIED);
  }

  @Test
  public void eqMissingPartitionKey() {

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "update_missingPartitionKeys()", WhereCQLClauseAnalyzer.StatementType.UPDATE);
    fixture
        .expression()
        .eqSkipOnePartitionKeys(0)
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(FilterException.Code.PRIMARY_KEY_NOT_FULLY_SPECIFIED);
  }
}
