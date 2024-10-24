package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import org.junit.jupiter.api.Test;

/** Tests for the {@link WhereCQLClauseAnalyzer}. Focus on Select Statement type */
public class TableUpdateAnalyzerTest {

  private static final TestData TEST_DATA = new TestData();

  private TestDataNames names() {
    return TEST_DATA.names;
  }

  // ==================================================================================================================
  // EASY CASES
  // ==================================================================================================================

  @Test
  public void updateOnKnownColumn() {
    var fixture =
        TEST_DATA.tableUpdateAnalyzer().table2PK3Clustering1Index("updateOnKnownColumn()");
    fixture.columnAssignments().setOnKnownColumn().analyze().assertNoUpdateException();
  }

  @Test
  public void updateOnPrimaryKey() {
    var fixture = TEST_DATA.tableUpdateAnalyzer().table2PK3Clustering1Index("updateOnPrimaryKey()");
    fixture
        .columnAssignments()
        .setOnPrimaryKeys()
        .analyzeThrows(UpdateException.class)
        .assertUpdateExceptionCode(UpdateException.Code.UPDATE_PRIMARY_KEY_COLUMNS);
  }

  @Test
  public void updateOnUnknownColumn() {
    var fixture =
        TEST_DATA.tableUpdateAnalyzer().table2PK3Clustering1Index("updateOnUnknownColumn()");
    var unknownColumnIdentifier =
        CqlIdentifierUtil.cqlIdentifierFromUserInput("column" + System.currentTimeMillis());
    fixture
        .columnAssignments()
        .setOnUnknownColumn(unknownColumnIdentifier)
        .analyzeThrows(UpdateException.class)
        .assertUpdateExceptionCode(UpdateException.Code.UNKNOWN_TABLE_COLUMNS);
  }
}
