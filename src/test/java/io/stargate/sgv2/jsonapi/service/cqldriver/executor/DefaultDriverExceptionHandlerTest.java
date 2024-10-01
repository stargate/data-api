package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link DefaultDriverExceptionHandler} this will also test the interface {@link
 * io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler}.
 */
public class DefaultDriverExceptionHandlerTest {

  private static final DefaultDriverExceptionHandlerTestData TEST_DATA =
      new DefaultDriverExceptionHandlerTestData();

  @ParameterizedTest
  @MethodSource("tableDriverErrorHandledData")
  public void tableDriverErrorHandled(
      DriverException originalEx,
      String expectedCode,
      boolean assertNames,
      boolean assertSchema,
      boolean assertMessage) {

    var handledEx =
        assertDoesNotThrow(
            () -> TEST_DATA.TABLE_HANDLER.maybeHandle(TEST_DATA.TABLE_SCHEMA_OBJECT, originalEx));

    assertThat(handledEx).isNotNull();

    assertThat(handledEx)
        .as(
            "Handled error should be a different instance, all driver errors map to a different exception")
        .isNotSameAs(originalEx);

    // for now, assumed they always turn into a DatabaseException
    assertThat(handledEx)
        .as("Handled error is of expected class")
        .isOfAnyClassIn(DatabaseException.class);

    DatabaseException apiEx = (DatabaseException) handledEx;
    assertThat(apiEx.code).as("Handled error has the expected code").isEqualTo(expectedCode);

    if (assertNames) {
      assertThat(apiEx)
          .as("Handled error message has the schema names")
          .hasMessageContaining(errFmt(TEST_DATA.KEYSPACE_NAME))
          .hasMessageContaining(errFmt(TEST_DATA.TABLE_NAME));
    }

    if (assertSchema) {
      assertThat(apiEx)
          .as("Handled error message has the schema object type name")
          .hasMessageContaining(TEST_DATA.TABLE_SCHEMA_OBJECT.type.name());
    }

    if (assertMessage) {
      assertThat(apiEx)
          .as("Handled error message the message from the original exception")
          .hasMessageContaining(originalEx.getMessage());
    }
  }

  private static Stream<Arguments> tableDriverErrorHandledData() {
    DefaultNode mockCoordinator = mock(DefaultNode.class);
    return Stream.of(
        // Test for CLOSED_CONNECTION
        Arguments.of(
            new ClosedConnectionException("closed"),
            DatabaseException.Code.CLOSED_CONNECTION.name(),
            true,
            true,
            true),
        // Test for READ_FAILURE
        Arguments.of(
            new ReadFailureException(
                mockCoordinator, DefaultConsistencyLevel.LOCAL_QUORUM, 1, 2, 1, true, Map.of()),
            DatabaseException.Code.READ_FAILURE.name(),
            true,
            true,
            false),
        // Test for READ_TIMEOUT
        Arguments.of(
            new ReadTimeoutException(
                mockCoordinator, DefaultConsistencyLevel.LOCAL_QUORUM, 1, 2, true),
            DatabaseException.Code.READ_TIMEOUT.name(),
            true,
            true,
            false),
        // Test for WRITE_FAILURE
        Arguments.of(
            new WriteFailureException(
                mockCoordinator,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                1,
                2,
                WriteType.SIMPLE,
                1,
                Map.of()),
            DatabaseException.Code.WRITE_FAILURE.name(),
            true,
            true,
            false),
        // Test for WRITE_TIMEOUT
        Arguments.of(
            new WriteTimeoutException(
                mockCoordinator, DefaultConsistencyLevel.LOCAL_QUORUM, 1, 2, WriteType.SIMPLE),
            DatabaseException.Code.WRITE_TIMEOUT.name(),
            true,
            true,
            false));
  }
}
