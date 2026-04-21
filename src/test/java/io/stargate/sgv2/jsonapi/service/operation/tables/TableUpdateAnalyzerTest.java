package io.stargate.sgv2.jsonapi.service.operation.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

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
    fixture
        .columnAssignments()
        .setOnKnownColumn(fixture.tableSchemaObject)
        .analyze()
        .assertNoUpdateException();
  }

  @Test
  public void updateOnPrimaryKey() {
    var fixture = TEST_DATA.tableUpdateAnalyzer().table2PK3Clustering1Index("updateOnPrimaryKey()");
    fixture
        .columnAssignments()
        .setOnPrimaryKeys(fixture.tableSchemaObject)
        .analyzeThrows(UpdateException.class)
        .assertUpdateExceptionCode(UpdateException.Code.UNSUPPORTED_UPDATE_FOR_PRIMARY_KEY_COLUMNS);
  }

  @Test
  public void updateOnUnknownColumn() {
    var fixture =
        TEST_DATA.tableUpdateAnalyzer().table2PK3Clustering1Index("updateOnUnknownColumn()");
    var unknownColumnIdentifier =
        CqlIdentifierUtil.cqlIdentifierFromUserInput("column" + System.currentTimeMillis());

    // unknown column is now caught when binding / preparing the values
    var exception =
        assertThrowsExactly(
            UpdateException.class,
            () ->
                fixture
                    .columnAssignments()
                    .setOnUnknownColumn(fixture.tableSchemaObject, unknownColumnIdentifier));

    var expectedCode = UpdateException.Code.UNKNOWN_TABLE_COLUMNS;
    assertThat(exception)
        .as("UpdateException with code %s when: %s".formatted(expectedCode, fixture.message))
        .isInstanceOf(UpdateException.class)
        .satisfies(e -> assertThat(e.code).isEqualTo(expectedCode.name()));
  }
}
