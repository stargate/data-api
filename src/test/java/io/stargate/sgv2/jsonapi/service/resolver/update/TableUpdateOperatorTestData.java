package io.stargate.sgv2.jsonapi.service.resolver.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataSuplier;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableUpdateOperatorTestData extends TestDataSuplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableUpdateOperatorTestData.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  public TableUpdateOperatorTestData(TestData testData) {
    super(testData);
  }

  public TableUpdateOperatorResolverFixture tableWithMapSetList(
      TableUpdateOperatorResolver tableUpdateOperatorResolver,
      UpdateOperator updateOperator,
      String message) {

    var table = testData.schemaObject().tableWithMapSetList();
    return new TableUpdateOperatorResolverFixture(
        table, tableUpdateOperatorResolver, updateOperator, message);
  }

  public static class TableUpdateOperatorResolverFixture implements Recordable {

    private String message;
    private String operatorJson = null;
    private final TableMetadata tableMetadata;
    private final TableSchemaObject tableSchemaObject;
    private final TableUpdateOperatorResolver tableUpdateOperatorResolver;
    private final UpdateOperator updateOperator;
    private List<ColumnAssignment> columnAssignments = null;
    public Throwable exception = null;

    public TableUpdateOperatorResolverFixture(
        TableSchemaObject table,
        TableUpdateOperatorResolver tableUpdateOperatorResolver,
        UpdateOperator updateOperator,
        String message) {
      this.tableMetadata = table.tableMetadata();
      this.tableSchemaObject = table;
      this.tableUpdateOperatorResolver = tableUpdateOperatorResolver;
      this.updateOperator = updateOperator;
      this.message = message;
    }

    public <T extends Throwable> TableUpdateOperatorResolverFixture resolveWithError(
        String operatorJson,
        Class<T> exceptionClass,
        UpdateException.Code code,
        String... errorSnippets)
        throws JsonProcessingException {
      this.operatorJson = operatorJson;
      this.exception =
          assertThrowsExactly(
              exceptionClass,
              this::callResolve,
              "Expected exception %s when: %s".formatted(exceptionClass, message));

      assertThat(exception)
          .as("UpdateException with code %s when: %s".formatted(code, message))
          .isInstanceOf(UpdateException.class)
          .satisfies(
              e -> {
                assertThat(((UpdateException) e).code).isEqualTo(code.name());
                for (String snippet : errorSnippets) {
                  assertThat(e.getMessage()).contains(snippet);
                }
              });

      return this;
    }

    public TableUpdateOperatorResolverFixture resolve(String operatorJson) {
      this.operatorJson = operatorJson;
      assertDoesNotThrow(this::callResolve, "Unexpected exception when: %s".formatted(message));
      return this;
    }

    private void callResolve() throws JsonProcessingException {
      LOGGER.warn("Resolve the : {}\n {}", message, PrettyPrintable.pprint(this));

      columnAssignments =
          tableUpdateOperatorResolver.resolve(
              tableSchemaObject,
              new TableUpdateResolver.ErrorStrategy(updateOperator),
              (ObjectNode) mapper.readTree(operatorJson));
    }

    public TableUpdateOperatorResolverFixture assertSingleAssignment() {
      assertThat(columnAssignments).hasSize(1);
      return this;
    }

    public TableUpdateOperatorResolverFixture assertFirstAssignmentEqual(
        UpdateOperator operator, Object expectedValue) {

      assertThat(columnAssignments.getFirst().namedValue().value()).isEqualTo(expectedValue);
      return this;
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("table", tableMetadata.describe(true))
          .append("operatorJson", operatorJson)
          .append("columnAssignments", columnAssignments)
          .append("exception", exception);
    }
  }
}
