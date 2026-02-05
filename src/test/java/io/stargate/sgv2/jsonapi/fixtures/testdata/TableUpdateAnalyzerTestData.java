package io.stargate.sgv2.jsonapi.fixtures.testdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdateAnalyzer;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableUpdateAnalyzerTestData extends TestDataSuplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableUpdateAnalyzerTestData.class);

  public TableUpdateAnalyzerTestData(TestData testData) {
    super(testData);
  }

  public TableUpdateAnalyzerFixture table2PK3Clustering1Index(String message) {
    var tableMetaData = testData.tableMetadata().table2PK3Clustering1Index();
    return new TableUpdateAnalyzerFixture(message, tableMetaData);
  }

  public static class TableUpdateAnalyzerFixture implements Recordable {

    public final String message;
    private final TableMetadata tableMetadata;
    public final TableSchemaObject tableSchemaObject;
    private final TableUpdateAnalyzer analyzer;
    private final UpdateClauseTestData.ColumnAssignmentsBuilder<TableUpdateAnalyzerFixture>
        columnAssignments;
    public Throwable exception = null;

    public TableUpdateAnalyzerFixture(String message, TableMetadata tableMetadata) {
      this.message = message;
      this.tableMetadata = tableMetadata;
      var TEST_CONSTANT = new TestConstants();
      this.analyzer =
          new TableUpdateAnalyzer(
              TableSchemaObject.from(TEST_CONSTANT.TENANT, tableMetadata, new ObjectMapper()));
      this.tableSchemaObject =
          TableSchemaObject.from(TEST_CONSTANT.TENANT, tableMetadata, new ObjectMapper());
      this.columnAssignments =
          new UpdateClauseTestData.ColumnAssignmentsBuilder<>(this, tableMetadata);
    }

    public UpdateClauseTestData.ColumnAssignmentsBuilder<TableUpdateAnalyzerFixture>
        columnAssignments() {
      return columnAssignments;
    }

    public TableUpdateAnalyzerFixture analyzeMaybeUpdateError(UpdateException.Code expectedCode) {
      if (expectedCode == null) {
        return analyze().assertNoUpdateException();
      }
      return analyzeThrows(FilterException.class).assertUpdateExceptionCode(expectedCode);
    }

    public <T extends Throwable> TableUpdateAnalyzerFixture analyzeThrows(Class<T> exceptionClass) {
      this.exception =
          assertThrowsExactly(
              exceptionClass,
              this::callAnalyze,
              "Expected exception %s when: %s".formatted(exceptionClass, message));

      LOGGER.warn("Analysis Error: {}\n {}", message, this.exception.toString());
      return this;
    }

    public TableUpdateAnalyzerFixture analyze() {
      if (columnAssignments.columnAssignments.isEmpty()) {
        throw new IllegalArgumentException(
            "Make sure construct the update ColumnAssignments first, before call analyze");
      }
      assertDoesNotThrow(this::callAnalyze, "No error when: %s".formatted(message));
      return this;
    }

    public void callAnalyze() {
      LOGGER.warn("Analyzing update clause: {}\n {}", message, PrettyPrintable.pprint(this));
      analyzer.analyze(columnAssignments.columnAssignments);
    }

    public TableUpdateAnalyzerFixture assertNoUpdateException() {
      assertThat(exception).isNull();
      return this;
    }

    public TableUpdateAnalyzerFixture assertUpdateExceptionCode(UpdateException.Code code) {
      if (code == null) {
        assertThat(exception).as("No UpdateException when: %s".formatted(message)).isNull();
      } else {
        assertThat(exception)
            .as("UpdateException with code %s when: %s".formatted(code, message))
            .isInstanceOf(UpdateException.class)
            .satisfies(e -> assertThat(((UpdateException) e).code).isEqualTo(code.name()));
      }
      return this;
    }

    @Override
    public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
      return dataRecorder
          .append("columnAssignments", columnAssignments.columnAssignments)
          .append("table", tableMetadata.describe(true));
    }
  }
}
