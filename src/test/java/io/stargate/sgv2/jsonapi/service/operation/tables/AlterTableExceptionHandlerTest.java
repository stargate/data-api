package io.stargate.sgv2.jsonapi.service.operation.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AlterTableExceptionHandler}. */
public class AlterTableExceptionHandlerTest {

  private static final TestConstants TEST_CONSTANTS = new TestConstants();

  @Test
  public void previouslyDroppedColumnError() {
    var handler =
        new AlterTableExceptionHandler(
            TEST_CONSTANTS.TABLE_SCHEMA_OBJECT, null, TEST_CONSTANTS.TABLE_IDENTIFIER.table());
    var exception =
        new InvalidQueryException(
            mock(Node.class),
            "Cannot re-add previously dropped column 'president' of type person, incompatible with previous type tuple<text, text>");

    var handled = handler.maybeHandle(exception);

    assertThat(handled).isInstanceOf(SchemaException.class);
    assertThat(((SchemaException) handled).code)
        .isEqualTo(SchemaException.Code.CANNOT_ADD_PREVIOUSLY_DROPPED_COLUMN.name());
    assertThat(handled)
        .hasMessageContaining("The command attempted to add the column: president.")
        .hasMessageContaining("The command used the column type: person.")
        .hasMessageContaining(
            "The database recorded the previously dropped column type as: tuple<text, text>.")
        .hasMessageContaining(TEST_CONSTANTS.KEYSPACE_NAME)
        .hasMessageContaining(TEST_CONSTANTS.TABLE_NAME)
        .hasMessageContaining("Resend the command using a different column name");
  }
}
