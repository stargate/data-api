package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;
import static io.stargate.sgv2.jsonapi.exception.ExceptionAction.EVICT_SESSION_CACHE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.ConnectionInitException;
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
import io.stargate.sgv2.jsonapi.exception.ExceptionAction;
import io.stargate.sgv2.jsonapi.util.CqlPrintUtil;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link DefaultDriverExceptionHandler} see also {@link DriverExceptionHandlerTest} for
 * tests on the interface itself
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

    var handledEx = assertDoesNotThrow(() -> TEST_DATA.DRIVER_HANDLER.maybeHandle(originalEx));

    LOGGER.info(
        "Handled Exception: \n{}", PrettyPrintable.pprint(new TestResult(originalEx, handledEx)));

    assertThat(handledEx).as("Handled exceptions is not null").isNotNull();

    assertThat(handledEx)
        .as("Handled error is different object to original")
        .isNotSameAs(originalEx);

    // for now, assumed they always turn into a DatabaseException
    assertThat(handledEx)
        .as("Handled error is of assertions class")
        .isOfAnyClassIn(DatabaseException.class);

    DatabaseException apiEx = (DatabaseException) handledEx;
    assertions.runAssertions(TEST_DATA, originalEx, apiEx);
  }

  public record TestResult(DriverException originalException, RuntimeException handledException)
      implements Recordable {

    @Override
    public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
      return dataRecorder
          .append("originalException.simpleClass", originalException.getClass().getSimpleName())
          .append("originalException.message", originalException.getMessage())
          .append("originalException.cause", originalException.getCause())
          .append("handledException", handledException);
    }
  }

  public record Assertions(
      DatabaseException.Code expectedCode,
      boolean assertSchemaNames,
      boolean assertOrigError,
      boolean assertCql,
      String assertMessage,
      EnumSet<ExceptionAction> assertExceptionActions) {

    public static Assertions of(DatabaseException.Code code) {
      return new Assertions(code, true, false, false, null, null);
    }

    public static Assertions of(DatabaseException.Code code, String assertMessage) {
      return new Assertions(code, true, false, false, assertMessage, null);
    }

    public static Assertions of(
        DatabaseException.Code code, EnumSet<ExceptionAction> assertExceptionActions) {
      return new Assertions(code, true, false, false, null, assertExceptionActions);
    }

    public static Assertions of(
        DatabaseException.Code code,
        String assertMessage,
        EnumSet<ExceptionAction> assertExceptionActions) {
      return new Assertions(code, true, false, false, assertMessage, assertExceptionActions);
    }

    public static Assertions isUnexpectedDriverException() {
      return new Assertions(
          DatabaseException.Code.UNEXPECTED_DRIVER_ERROR,
          true,
          true,
          false,
          null,
          EnumSet.of(EVICT_SESSION_CACHE));
    }

    /**
     * Assert the API exception has the message from the exception, not the original exception. Used
     * when we expect to unpack a cause
     *
     * @return
     */
    public static Assertions isUnexpectedDriverException(RuntimeException cause) {
      return new Assertions(
          DatabaseException.Code.UNEXPECTED_DRIVER_ERROR,
          true,
          false,
          false,
          cause.getMessage(),
          null);
    }

    public void runAssertions(
        DefaultDriverExceptionHandlerTestData testData,
        DriverException originalException,
        APIException handledException) {
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

      if (assertCql) {
        assertThat(handledException)
            .as("Handled error message contains cql statement")
            .hasMessageContaining(CqlPrintUtil.trimmedCql(testData.STATEMENT))
            .hasMessageContaining(
                errFmtJoin(
                    CqlPrintUtil.trimmedPositionalValues(testData.STATEMENT), Object::toString));
      }

      if (!Strings.isNullOrEmpty(assertMessage)) {
        assertThat(handledException)
            .as("Handled error message contains assertions message")
            .hasMessageContaining(assertMessage);
      }

      if (assertExceptionActions != null) {
        assertThat(handledException.exceptionActions)
            .as("Handled error should have expected exception actions")
            .isEqualTo(assertExceptionActions);
      }
    }

    @Override
    public String toString() {
      // simplified to be called from the TestArguments
      return String.format(
          "expectedCode='%s', assertSchemaNames=%s, assertMessage='%s', assertExceptionActions=%s",
          expectedCode, assertSchemaNames, assertMessage, assertExceptionActions);
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
            new ClosedConnectionException("closed"), Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new CodecNotFoundException(DataTypes.TEXT, GenericType.STRING),
            Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new DriverExecutionException(new ClosedConnectionException("closed")),
            Assertions.isUnexpectedDriverException(new ClosedConnectionException("closed"))),
        new TestArguments(
            new DriverExecutionException(null), Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new DriverTimeoutException("timeout"), Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new InvalidKeyspaceException("invalid keyspace: monkeys"),
            new Assertions(
                DatabaseException.Code.UNKNOWN_KEYSPACE,
                false,
                false,
                false,
                TEST_DATA.KEYSPACE_NAME.asCql(true),
                null)),
        new TestArguments(
            new NodeUnavailableException(mockNode("node: monkeys")),
            Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new RequestThrottlingException("throttled"), Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new UnsupportedProtocolVersionException(null, "unsupported protocol", List.of()),
            Assertions.isUnexpectedDriverException()),
        new TestArguments(
            allFailedTwoNodesOneWriteTimeout(),
            Assertions.of(DatabaseException.Code.TIMEOUT_WRITING_DATA)),
        new TestArguments(
            allFailedTwoNodesAllAuth(), Assertions.of(DatabaseException.Code.UNAUTHORIZED_ACCESS)),
        new TestArguments(
            allFailedOneRuntime(),
            Assertions.of(
                DatabaseException.Code.UNEXPECTED_DRIVER_ERROR,
                "unexpected runtime",
                EnumSet.of(EVICT_SESSION_CACHE))),
        new TestArguments(
            allFailedUnexpectedDriverError(),
            Assertions.of(
                DatabaseException.Code.UNEXPECTED_DRIVER_ERROR,
                "closed",
                EnumSet.of(EVICT_SESSION_CACHE))),
        new TestArguments(
            allFailedClusterNodeRecycled(),
            Assertions.of(
                DatabaseException.Code.UNEXPECTED_DRIVER_ERROR,
                "cluster node recycled, unable to connect to it",
                EnumSet.of(EVICT_SESSION_CACHE))),
        new TestArguments(
            new NoNodeAvailableException(),
            Assertions.of(
                DatabaseException.Code.FAILED_TO_CONNECT_TO_DATABASE,
                "unable to connect to any nodes",
                EnumSet.of(EVICT_SESSION_CACHE))),
        // AlreadyExistsException should be handled by a specific subclass that knows the type of
        // command
        // see CreateTableExceptionHandler
        new TestArguments(
            new AlreadyExistsException(
                mockNode("node: monkeys"),
                TEST_DATA.KEYSPACE_NAME.asCql(true),
                TEST_DATA.TABLE_NAME.asCql(true)),
            Assertions.isUnexpectedDriverException()),
        // InvalidConfigurationInQueryException will happen if we send wrong DDL command
        // not expected
        new TestArguments(
            new InvalidConfigurationInQueryException(mockNode("node: monkeys"), "bad DDL config"),
            Assertions.isUnexpectedDriverException()),
        new TestArguments(
            new InvalidQueryException(mockNode("node1"), "Invalid CQL"),
            new Assertions(
                DatabaseException.Code.INVALID_DATABASE_QUERY,
                true,
                false,
                true,
                "Invalid CQL",
                null)),
        new TestArguments(
            new SyntaxError(mockNode("node1"), "Syntax Error CQL"),
            new Assertions(
                DatabaseException.Code.UNSUPPORTED_DATABASE_QUERY,
                true,
                false,
                true,
                "Syntax Error CQL",
                null)),
        new TestArguments(
            new CASWriteUnknownException(mockNode("node1"), ConsistencyLevel.QUORUM, 1, 2),
            new Assertions(
                DatabaseException.Code.FAILED_COMPARE_AND_SET,
                true,
                false,
                true,
                "CAS operation result is unknown - proposal was not accepted by a quorum.",
                null)),
        new TestArguments(
            new TruncateException(
                mockNode("node1"),
                "Error during truncate: Truncate Error"), // See TruncateException in C* code
            new Assertions(
                DatabaseException.Code.FAILED_TRUNCATION,
                true,
                false,
                true,
                "Error during truncate: Truncate Error",
                null)),
        new TestArguments(
            new UnavailableException(mockNode("node1"), ConsistencyLevel.QUORUM, 2, 1),
            new Assertions(
                DatabaseException.Code.UNAVAILABLE_DATABASE,
                true,
                false,
                false,
                "Not enough replicas available for query at consistency QUORUM (2 required but only 1 alive)",
                null)),
        new TestArguments(
            new ReadFailureException(
                mockNode("node1"),
                ConsistencyLevel.QUORUM,
                1,
                2,
                2,
                true,
                Map.of(
                    InetAddress.getLoopbackAddress(),
                    RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code,
                    getInetAddress(new byte[] {(byte) 192, (byte) 168, 0, 1}),
                    RequestFailureReason.INDEX_NOT_AVAILABLE.code)),
            new Assertions(
                DatabaseException.Code.FAILED_READ_REQUEST,
                true,
                false,
                false,
                "Cassandra failure during read query at consistency QUORUM (2 responses were required but only 1 replica responded, 2 failed",
                null)),
        new TestArguments(
            new WriteFailureException(
                mockNode("node1"),
                ConsistencyLevel.QUORUM,
                1,
                2,
                WriteType.SIMPLE,
                2,
                Map.of(
                    InetAddress.getLoopbackAddress(),
                    RequestFailureReason.INCOMPATIBLE_SCHEMA.code,
                    getInetAddress(new byte[] {(byte) 192, (byte) 168, 0, 1}),
                    RequestFailureReason.UNKNOWN_COLUMN.code)),
            new Assertions(
                DatabaseException.Code.FAILED_WRITE_REQUEST,
                true,
                false,
                false,
                "Cassandra failure during write query at consistency QUORUM (2 responses were required but only 1 replica responded, 2 failed",
                null)),
        new TestArguments( // the AllNodesFailed test only checks the code is TIMEOUT this checks
            // the details of the error
            new WriteTimeoutException(
                mockNode("node1"), ConsistencyLevel.QUORUM, 1, 2, WriteType.SIMPLE),
            new Assertions(
                DatabaseException.Code.TIMEOUT_WRITING_DATA, true, false, true, null, null)),
        new TestArguments(
            new ReadTimeoutException(mockNode("node1"), ConsistencyLevel.QUORUM, 1, 2, false),
            new Assertions(
                DatabaseException.Code.TIMEOUT_READING_DATA, true, false, true, null, null)));
  }

  private static InetAddress getInetAddress(byte[] address) {
    try {
      return InetAddress.getByAddress(address);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static AllNodesFailedException allFailedTwoNodesOneWriteTimeout() {

    var node1 = mockNode("node1");
    var node1Ex = new WriteTimeoutException(node1, ConsistencyLevel.QUORUM, 1, 2, WriteType.SIMPLE);

    var node2 = mockNode("node2");
    var node2Ex = new OverloadedException(node2);

    return AllNodesFailedException.fromErrors(
        List.of(
            new AbstractMap.SimpleEntry<>(node1, node1Ex),
            new AbstractMap.SimpleEntry<>(node2, node2Ex)));
  }

  private static AllNodesFailedException allFailedTwoNodesAllAuth() {

    var node1 = mockNode("node1");
    var node1Ex = new UnauthorizedException(node1, "auth node 1");

    var node2 = mockNode("node2");
    var node2Ex = new UnauthorizedException(node2, "auth node 2");

    return AllNodesFailedException.fromErrors(
        List.of(
            new AbstractMap.SimpleEntry<>(node1, node1Ex),
            new AbstractMap.SimpleEntry<>(node2, node2Ex)));
  }

  private static AllNodesFailedException allFailedOneRuntime() {
    // the AllNodes exception uses throwable, not sure how /when they can happen but handling it
    // these both have same priority, so the first one should be used
    var node1 = mockNode("node1");
    var node1Ex = new RuntimeException("unexpected runtime");

    var node2 = mockNode("node2");
    var node2Ex = new ClosedConnectionException("closed");

    return AllNodesFailedException.fromErrors(
        List.of(
            new AbstractMap.SimpleEntry<>(node1, node1Ex),
            new AbstractMap.SimpleEntry<>(node2, node2Ex)));
  }

  private static AllNodesFailedException allFailedUnexpectedDriverError() {
    // using an error here that we expect to map to the UNEXPECTED_DRIVER_ERROR
    var node1 = mockNode("node1");
    var node1Ex = new ClosedConnectionException("closed");

    var node2 = mockNode("node2");
    var node2Ex = new ClosedConnectionException("closed");

    return AllNodesFailedException.fromErrors(
        List.of(
            new AbstractMap.SimpleEntry<>(node1, node1Ex),
            new AbstractMap.SimpleEntry<>(node2, node2Ex)));
  }

  private static AllNodesFailedException allFailedClusterNodeRecycled() {
    // using an error here that we expect to map to the UNEXPECTED_DRIVER_ERROR
    var node1 = mockNode("node1");
    var node1Ex =
        new ConnectionInitException(
            "cluster node recycled, unable to connect to it", new ClosedChannelException());

    var node2 = mockNode("node2");
    var node2Ex = new UnknownHostException("unknown host");

    return AllNodesFailedException.fromErrors(
        List.of(
            new AbstractMap.SimpleEntry<>(node1, node1Ex),
            new AbstractMap.SimpleEntry<>(node2, node2Ex)));
  }

  /**
   * Returns a fake {@link Node} that returns the message for toString
   *
   * <p>The errors only use toString (that we have seen so far)
   */
  private static Node mockNode(String message) {
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
