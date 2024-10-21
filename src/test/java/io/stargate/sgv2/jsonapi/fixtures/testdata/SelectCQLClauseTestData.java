package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.querybuilder.select.OngoingSelection;
import io.stargate.sgv2.jsonapi.service.operation.query.SelectCQLClause;

public class SelectCQLClauseTestData extends TestDataSuplier {

  public SelectCQLClauseTestData(TestData testData) {
    super(testData);
  }

  public SelectCQLClause selectAllFromTable() {
    return OngoingSelection::all;
  }
}
