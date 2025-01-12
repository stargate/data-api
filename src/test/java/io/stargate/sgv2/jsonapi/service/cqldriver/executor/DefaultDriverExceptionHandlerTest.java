package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.base.Strings;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.exception.ErrorTemplate;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link DefaultDriverExceptionHandler} this will also test the interface {@link
 * io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler}.
 */
public class DefaultDriverExceptionHandlerTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DefaultDriverExceptionHandlerTest.class);

  private static final DefaultDriverExceptionHandlerTestData TEST_DATA =
      new DefaultDriverExceptionHandlerTestData();

  @ParameterizedTest
  @MethodSource("tableDriverErrorHandledData")
  public void tableDriverErrorHandled(TestArguments arguments) {

    var originalEx = arguments.originalEx;
    var assertions = arguments.assertions;

    var handledEx =
        assertDoesNotThrow(
            () -> TEST_DATA.DRIVER_HANDLER.maybeHandle(TEST_DATA.TABLE_SCHEMA_OBJECT, originalEx));

    var prettyString = new TestResult(originalEx, handledEx).toString(true);
    LOGGER.info("Handled Exception: \n{}", prettyString);

    assertThat(handledEx).as("Handled exceptions is not null").isNotNull();

    assertThat(handledEx)
        .as("Handled error is different object to original")
        .isNotSameAs(originalEx);

    // for now, assumed they always turn into a DatabaseException
    assertThat(handledEx)
        .as("Handled error is of assertions class")
        .isOfAnyClassIn(DatabaseException.class);

    DatabaseException apiEx = (DatabaseException) handledEx;
    assertions.runAssertions(originalEx, apiEx);
  }

  public record TestResult(DriverException originalException, RuntimeException handledException)
      implements PrettyPrintable {

    @Override
    public PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
      prettyToStringBuilder
          .append("originalException.simpleClass", originalException.getClass().getSimpleName())
          .append("originalException.message", originalException.getMessage())
          .append("originalException.cause", originalException.getCause())
          .append("handledException", handledException);
      return prettyToStringBuilder;
    }
  }

  public record Assertions(
      DatabaseException.Code expectedCode,
      boolean assertSchemaNames,
      boolean assertOrigError,
      String assertMessage) {

    public static Assertions of(DatabaseException.Code code) {
      return new Assertions(code, true, false, null);
    }

    public static Assertions of(DatabaseException.Code code, String assertMessage) {
      return new Assertions(code, true,  false, assertMessage);
    }

    public static Assertions isUnexpectedDriverException() {
      return new Assertions(
          DatabaseException.Code.UNEXPECTED_DRIVER_ERROR, true, true, null);
    }

    public void runAssertions(DriverException originalException, APIException handledException) {
      assertThat(handledException.code)
          .as("Handled error has the assertions code")
          .isEqualTo(expectedCode.name());

      if (assertSchemaNames) {
        assertThat(handledException)
            .as("Handled error message has the schema names")
            .hasMessageContaining(errFmt(TEST_DATA.KEYSPACE_NAME))
            .hasMessageContaining(errFmt(TEST_DATA.TABLE_NAME));
      }

      if (assertOrigError) {
        assertThat(handledException)
            .as("Handled error message contains original error class")
            .hasMessageContaining(originalException.getClass().getSimpleName());

        // message is sometimes null if  the error is just a container for a cause
        assertThat(handledException)
            .as("Handled error message contains original error message")
            .hasMessageContaining(ErrorTemplate.replaceIfNull(originalException.getMessage()));
      }

      if (!Strings.isNullOrEmpty(assertMessage)) {
        assertThat(handledException)
            .as("Handled error message contains assertions message")
            .hasMessageContaining(assertMessage);
      }
    }

    @Override
    public String toString() {
      // simplified to be called from the TestArguments
      return String.format(
          "expectedCode='%s', assertSchemaNames=%s, assertMessage='%s'",
          expectedCode, assertSchemaNames, assertMessage);
    }
  }

  public record TestArguments(DriverException originalEx, Assertions assertions) {

    public Object[] get() {
      return new Object[] {originalEx, assertions};
    }

    @Override
    public String toString() {
      return String.format(
          "TestArguments{originalEx=%s, %s}",
          originalEx.getClass().getSimpleName(), assertions.toString());
    }
  }

  private static Stream<TestArguments> tableDriverErrorHandledData() {


    return Stream.of(
        new TestArguments(
            new ClosedConnectionException("closed"),
            Assertions.of(DatabaseException.Code.CLOSED_CONNECTION)),
        new TestArguments(
            new CodecNotFoundException(DataTypes.TEXT, GenericType.STRING),
            Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new DriverExecutionException(new ClosedConnectionException("closed")),
            Assertions.of(DatabaseException.Code.CLOSED_CONNECTION)),
        new TestArguments(
            new DriverExecutionException(null), Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new DriverTimeoutException("timeout"), Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new InvalidKeyspaceException("invalid keyspace: monkeys"),
            new Assertions(DatabaseException.Code.UNKNOWN_KEYSPACE, false, false, TEST_DATA.KEYSPACE_NAME.asCql(true))),
        new TestArguments(
            new NodeUnavailableException(mockNode("node: monkeys")),
            Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new RequestThrottlingException("throttled"),
            Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new UnsupportedProtocolVersionException(null, "unsupported protocol", List.of()),
            Assertions.isUnexpectedDriverException()),
        new TestArguments(
            allFailedTwoNodesOneWriteTimeout(),
            Assertions.of(DatabaseException.Code.TABLE_WRITE_TIMEOUT)),
        new TestArguments(
            allFailedTwoNodesAllAuth(),
            Assertions.of(DatabaseException.Code.UNAUTHORIZED_ACCESS)),
        new TestArguments(
            allFailedOneRuntime(),
            Assertions.of(DatabaseException.Code.UNEXPECTED_DRIVER_ERROR, "unexpected runtime")),
        new TestArguments(
            new NoNodeAvailableException(),
            Assertions.of(DatabaseException.Code.UNAVAILABLE_DATABASE)),
        // AlreadyExistsException should be handled by a specific subclass that knows the type of command
        // see CreateTableExceptionHandler
        new TestArguments(
            new AlreadyExistsException(mockNode("node: monkeys"), TEST_DATA.KEYSPACE_NAME.asCql(true), TEST_DATA.TABLE_NAME.asCql(true)),
            Assertions.isUnexpectedDriverException()),
        // InvalidConfigurationInQueryException will happen if we send wrong DDL command
        // not expected
    new TestArguments(
        new InvalidConfigurationInQueryException(mockNode("node: monkeys"), "bad DDL config"),
        Assertions.isUnexpectedDriverException())
    );
  }

  private static AllNodesFailedException allFailedTwoNodesOneWriteTimeout() {

    var node1 = mockNode("node1");
    var node1Ex = new WriteTimeoutException(node1, ConsistencyLevel.QUORUM, 1, 2, WriteType.SIMPLE);

    var node2 = mockNode("node2");
    var node2Ex = new OverloadedException(node2);

    return AllNodesFailedException.fromErrors(List.of(
        new AbstractMap.SimpleEntry<>(node1, node1Ex),
        new AbstractMap.SimpleEntry<>(node2, node2Ex)
    ));
  }

  private static AllNodesFailedException allFailedTwoNodesAllAuth() {

    var node1 = mockNode("node1");
    var node1Ex = new UnauthorizedException(node1, "auth node 1");

    var node2 = mockNode("node2");
    var node2Ex = new UnauthorizedException(node2, "auth node 2");

    return AllNodesFailedException.fromErrors(List.of(
        new AbstractMap.SimpleEntry<>(node1, node1Ex),
        new AbstractMap.SimpleEntry<>(node2, node2Ex)
    ));
  }

  private static AllNodesFailedException allFailedOneRuntime() {
    // the AllNodes exception uses throwable, not sure how /when they can happen but handling it
    // these both have same priority, so the first one should be used
    var node1 = mockNode("node1");
    var node1Ex = new RuntimeException("unexpected runtime");

    var node2 = mockNode("node2");
    var node2Ex = new ClosedConnectionException("closed");

    return AllNodesFailedException.fromErrors(List.of(
        new AbstractMap.SimpleEntry<>(node1, node1Ex),
        new AbstractMap.SimpleEntry<>(node2, node2Ex)
    ));
  }

  /**
   * Returns a fake {@link Node} that returns the message for toString
   * <p>
   * The errors only use toString (that we have seen so far)
   */
  private static Node mockNode(String message){
    return new Node() {
      @Override
      public String toString() {
        return message;
      }

      @Override
      public @NotNull EndPoint getEndPoint() {
        return null;
      }

      @Override
      public @NotNull Optional<InetSocketAddress> getBroadcastRpcAddress() {
        return Optional.empty();
      }

      @Override
      public @NotNull Optional<InetSocketAddress> getBroadcastAddress() {
        return Optional.empty();
      }

      @Override
      public @NotNull Optional<InetSocketAddress> getListenAddress() {
        return Optional.empty();
      }

      @Override
      public @Nullable String getDatacenter() {
        return "";
      }

      @Override
      public @Nullable String getRack() {
        return "";
      }

      @Override
      public @Nullable Version getCassandraVersion() {
        return null;
      }

      @Override
      public @NotNull Map<String, Object> getExtras() {
        return Map.of();
      }

      @Override
      public @NotNull NodeState getState() {
        return null;
      }

      @Override
      public long getUpSinceMillis() {
        return 0;
      }

      @Override
      public int getOpenConnections() {
        return 0;
      }

      @Override
      public boolean isReconnecting() {
        return false;
      }

      @Override
      public @NotNull NodeDistance getDistance() {
        return null;
      }

      @Override
      public @Nullable UUID getHostId() {
        return null;
      }

      @Override
      public @Nullable UUID getSchemaVersion() {
        return null;
      }
    };
  }
}
