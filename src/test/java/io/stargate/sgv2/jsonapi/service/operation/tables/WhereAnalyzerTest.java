package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import org.junit.jupiter.api.Test;

public class WhereAnalyzerTest {

  private static final TestData TEST_DATA = new TestData();

  @Test
  public void eqFilterOnAllPartitionKeys() {
    var fixture = TEST_DATA.whereAnalyzer().eqAllPartitionKeys();
    fixture
        .analyze()
        .assertAnalysisResultNotNull()
        .assertNotAllowFiltering()
        .assertNoWarningExceptions();
  }

  @Test
  public void eqFilterOnAllPrimaryKeys() {
    // This includes filtering on all partitionKeys and clusteringKeys
    var fixture = TEST_DATA.whereAnalyzer().eqAllPrimaryKeys();
    fixture
        .analyze()
        .assertAnalysisResultNotNull()
        .assertNotAllowFiltering()
        .assertNoWarningExceptions();
  }

  @Test
  public void eqFilterOnSkippingClusteringKeys() {
    // This fixture should have 3 clustering keys in table, and in the API filter, we are skipping
    // the second one
    var fixture = TEST_DATA.whereAnalyzer().eqAllPartitionKeysSkipOneClusteringKey();
    fixture
        .analyze()
        .assertAnalysisResultNotNull()
        .assertAllowFiltering()
        .assertWarningExceptions(WarningException.Code.INCOMPLETE_PRIMARY_KEY_FILTER);
  }

  @Test
  public void eqMissingIndexOnRegularColumn() {
    // This is the filter against a column(not PK, without SAI) that exists on table.
    var fixture = TEST_DATA.whereAnalyzer().eqRegularColumnFilterWithoutIndex();
    fixture
        .analyze()
        .assertAnalysisResultNotNull()
        .assertAllowFiltering()
        .assertWarningExceptions(WarningException.Code.MISSING_INDEX);
  }

  @Test
  public void emptyFilter() {
    // This is empty filter, ALLOW FILTERING, ZERO_FILTER_OPERATIONS
    var fixture = TEST_DATA.whereAnalyzer().emptyFilter();
    fixture
        .analyze()
        .assertAnalysisResultNotNull()
        .assertAllowFiltering()
        .assertWarningExceptions(WarningException.Code.ZERO_FILTER_OPERATIONS);
  }
}
