package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.*;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.util.CqlPrintUtil;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link DriverExceptionHandler} interface, we keep the interface so
 * all the type casting is done in one place and this class only worries about processing the
 * errors.
 *
 * <p>This class should cover almost all the driver exceptions, we create subclasses like the {@link
 * io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexExceptionHandler} to handle errors
 * in a command specific way. e.g. how we want to handle already exists when creating an index.
 *
 * <p>Because there are times we want to know the SchemaObject and per request info such as the
 * actual statement we run we need to hand around a factory function, not actual objects, which can
 * be the constructor. See the {@link Factory} and {@link FactoryWithIdentifier} below.
 *
 * <p><b>NOTE:</b> Try to keep the <code>handle()</code> functions grouped like they are in the
 * interface. Please.
 *
 * @param <SchemaT> The type of schema object this handler is for.
 */
public class DefaultDriverExceptionHandler<SchemaT extends SchemaObject>
    implements DriverExceptionHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDriverExceptionHandler.class);

  protected final RequestContext requestContext;
  protected final SchemaObject schemaObject;
  // NOTE: to subclasses - the statement may be null, this happens when some Operations work with
  // metadata
  // rather than using statements.
  protected final SimpleStatement statement;
  protected final CQLSessionCache sessionCache;

  /**
   * Creates a new instance of the driver exception mapper.
   *
   * @param requestContext The context of the current request, for cache eviction when encountering
   *     AllNodesFailedException.
   * @param schemaObject The schema object to provide context for the errors, must not be null.
   * @param statement Optional statement that the error handler is being used with.
   * @param sessionCache The CQL session cache instance, for cache eviction when encountering
   *     AllNodesFailedException.
   */
  public DefaultDriverExceptionHandler(
      RequestContext requestContext,
      SchemaT schemaObject,
      SimpleStatement statement,
      CQLSessionCache sessionCache) {

    this.requestContext = Objects.requireNonNull(requestContext, "requestContext must not be null");
    this.schemaObject = Objects.requireNonNull(schemaObject, "schemaObject must not be null");
    // no null check, statement may be null
    this.statement = statement;
    this.sessionCache = Objects.requireNonNull(sessionCache, "sessionCache must not be null");
  }

  /** Lower priority is more important, used when examining a list of errors from nodes */
  private static int getExceptionPriority(Throwable exception) {
    return switch (exception) {
      case QueryConsistencyException e -> 0;
      case QueryExecutionException e -> 25;
      case QueryValidationException e -> 50;
      default -> Integer.MAX_VALUE;
    };
  }

  /**
   * Gets the highest priority error from the {@link AllNodesFailedException#getAllErrors()} so we
   * can handle that and ignore the others.
   *
   * @param exception The parent exception.
   * @return Most important exception to handle.
   */
  private static Optional<Throwable> findHighestPriority(AllNodesFailedException exception) {
    var allExceptions = exception.getAllErrors().values().stream().flatMap(List::stream).toList();
    var highest = findHighestPriority(allExceptions);

    if (LOGGER.isErrorEnabled()) {
      LOGGER.error(
          "AllNodesFailedException handled, highest priority exception was: {} original message: {}",
          highest,
          exception.getMessage());
    }
    return highest;
  }

  private static Optional<Throwable> findHighestPriority(List<Throwable> exceptions) {

    var sortedExceptions = new ArrayList<>(exceptions);
    sortedExceptions.sort(
        Comparator.comparingInt(DefaultDriverExceptionHandler::getExceptionPriority));
    return sortedExceptions.stream().findFirst();
  }

  /**
   * Any driver exception that is not handled (handler returns same exception instance) wil be
   * mapped to the {@link DatabaseException.Code#UNEXPECTED_DRIVER_ERROR}
   */
  @Override
  public RuntimeException handleUnhandled(DriverException exception) {
    return DatabaseException.Code.UNEXPECTED_DRIVER_ERROR.get(errVars(schemaObject, exception));
  }

  // ========================================================================
  // Direct subclasses of DriverException with no child
  // ========================================================================

  // Following exceptions fall back to the {@link #handleUnhandled(DriverException)}:
  //
  // - **ClosedConnectionException**
  //   - Driver will auto retry, so if this bubbles out it is unexpected.
  //
  // - **CodecNotFoundException**
  //    - Happens if we try to encode/decode a type that does not have a codec in the driver.
  //    - The codecs we use to go to/from JSON should prevent this from happening.
  //
  // - **DriverExecutionException**
  //    - A catchall from the driver, should not happen.
  //
  // - **DriverTimeoutException**
  //    - Timeout on prepare and other non-user commands, should not happen.
  //
  // - **NodeUnavailableException**
  //    - Occurs when the driver configuration for max connections or in-flight requests is hit.
  //    - Nothing the user can do to change this, and it is not expected to happen.
  //
  // - **RequestThrottlingException**
  //    - Used for driver-level request throttling, which we are not using.
  //
  // - **UnsupportedProtocolVersionException**
  //    - Should not happen, as we control the protocol version.

  @Override
  public RuntimeException handle(DriverExecutionException exception) {
    // see the docs, this is often a wrapper for checked exceptions so re-handle if this is the case
    // otherwise it is unexpected
    return switch (exception.getCause()) {
      case DriverException de -> maybeHandle(de); // handle the cause
      case null, default -> exception; // unhandled, return the same exception
    };
  }

  @Override
  public RuntimeException handle(InvalidKeyspaceException exception) {
    return DatabaseException.Code.UNKNOWN_KEYSPACE.get(errVars(schemaObject, exception));
  }

  // ========================================================================
  // AllNodesFailedException and subclasses
  // ========================================================================

  @Override
  public RuntimeException handle(AllNodesFailedException exception) {
    // Evict the session from the cache, as it's likely in a "zombie" state
    // where the driver's topology is completely stale.
    try {
      if (sessionCache.evictSession(requestContext)) {
        LOGGER.info(
            "Evicted session for tenant '{}' after AllNodesFailedException.",
            requestContext.getTenantId().orElse(""));
      }
    } catch (Exception e) {
      LOGGER.error(
          "Failed to evict session for tenant '{}' after AllNodesFailedException.",
          requestContext.getTenantId().orElse(""),
          e);
    }

    // Should always be created with errors from calling each node, re-process the most important
    // error
    var highestPriority = findHighestPriority(exception).orElseGet(() -> null);

    return switch (highestPriority) {
        // found a node specific error that is a driver based
      case DriverException e -> maybeHandle(e);
        // this is a non-driver based exception, so map to generic unexpected driver error
      case RuntimeException re ->
          DatabaseException.Code.UNEXPECTED_DRIVER_ERROR.get(errVars(schemaObject, re));
        // could not work out what the node error was OR this was a subclass of the
        // AllNodesFailedException
        // this will be the null case, but also need a default label
      case null -> DriverExceptionHandler.super.handle(exception);
      default -> DriverExceptionHandler.super.handle(exception);
    };
  }

  @Override
  public RuntimeException handle(NoNodeAvailableException exception) {
    // this is a special case of AllNodesFailedException where no nodes were available
    return DatabaseException.Code.FAILED_TO_CONNECT_TO_DATABASE.get(
        errVars(schemaObject, exception));
  }

  // ========================================================================
  // QueryValidationException and subclasses
  // - this is a subclass CoordinatorException but that is abstract
  // ========================================================================

  // Following exceptions fall back to the {@link #handleUnhandled(DriverException)}:
  //
  // - **AlreadyExistsException**
  //    - This should be handled by the subclasses of this handler because they know the specific
  // entity we are trying to create.
  //    - See {@link CreateTableDriverExceptionHandler} for an example.
  //
  // - **InvalidConfigurationInQueryException**
  //    - Happens if we get the DDL command wrong.
  //    - Should not happen.

  /**
   * this is the InvalidRequestException in the Cassandra code base, there are 300+ usages in it.
   * generally means while the syntax is correct, what you are asking it to do is not possible e.g.
   * add to a list, but the column is not a list. the Data API should prevent this from happening,
   * but if it does, it is a bug (for the API to let it through). The user may be able to work
   * around by trying a different request, but we are not able to give them specific help.
   */
  @Override
  public RuntimeException handle(InvalidQueryException exception) {
    return DatabaseException.Code.INVALID_DATABASE_QUERY.get(
        errVars(
            schemaObject,
            exception,
            map -> {
              map.put("cql", CqlPrintUtil.trimmedCql(statement));
              map.put(
                  "values",
                  errFmtJoin(CqlPrintUtil.trimmedPositionalValues(statement), Object::toString));
            }));
  }

  @Override
  public RuntimeException handle(SyntaxError exception) {
    // Really should not happen, we are using the query builder but handle it just incase.
    return DatabaseException.Code.UNSUPPORTED_DATABASE_QUERY.get(
        errVars(
            schemaObject,
            exception,
            map -> {
              map.put("cql", CqlPrintUtil.trimmedCql(statement));
              map.put(
                  "values",
                  errFmtJoin(CqlPrintUtil.trimmedPositionalValues(statement), Object::toString));
            }));
  }

  @Override
  public RuntimeException handle(UnauthorizedException exception) {
    return DatabaseException.Code.UNAUTHORIZED_ACCESS.get(errVars(schemaObject, exception));
  }

  // ========================================================================
  // QueryConsistencyException and subclasses
  // see commend for the QueryExecutionException section
  // ========================================================================

  // Following exceptions fall back to the {@link #handleUnhandled(DriverException)}:
  //
  // - **BootstrappingException**
  //    - This should be handled by the subclasses of this handler because they know the specific
  // entity we are trying to create.
  //    - See {@link CreateTableDriverExceptionHandler} for an example.
  // - **CDCWriteFailureException**
  //    - Is out of scope, and if a user has it enabled we assume they know what they are doing.
  // - **FunctionFailureException**
  //    - API is not using CQL functions, UDFs, or Custom functions, and we do not support them.
  // - **InvalidConfigurationInQueryException**
  //    - Happens if we get the DDL command wrong.
  //    - Should not happen.
  // - **OverloadedException**
  //    - Reading C* code this happens when either number of hints or when the
  // ReplicaFilteringProtection is working
  //      to repair inconsistency when queries use ALLOW FILTERING and the data is not consistent.
  // While we use
  //      ALLOW FILTERING, this is an overload protection so will wait to see it happen in the wild
  // before handling it.
  // - **QueryConsistencyException**
  //    - This is a parent for things like ReadFailureException, ReadTimeoutException, handle in
  // those.

  /**
   * Thrown when the LWT proposal was not accepted by a quorum of nodes, so the LWT transaction is
   * in an unknown state.
   *
   * <p>We should retry on this error in {@link
   * io.stargate.sgv2.jsonapi.service.cqldriver.CqlProxyRetryPolicy} and when we get to processing
   * it here it means we could not make the LWT work.
   *
   * @param exception
   * @return
   */
  @Override
  public RuntimeException handle(CASWriteUnknownException exception) {
    return DatabaseException.Code.FAILED_COMPARE_AND_SET.get(
        errVars(
            schemaObject,
            exception,
            map -> {
              map.put("cql", CqlPrintUtil.trimmedCql(statement));
              map.put(
                  "values",
                  errFmtJoin(CqlPrintUtil.trimmedPositionalValues(statement), Object::toString));
            }));
  }

  /**
   * Thrown when either UnavailableException | TimeoutException | InvalidRequestException happens in
   * C*.
   *
   * <p>This should retry in the {@link
   * io.stargate.sgv2.jsonapi.service.cqldriver.CqlProxyRetryPolicy} and if we get here we assume
   * there is a proper failure.
   *
   * @param exception
   * @return
   */
  @Override
  public RuntimeException handle(TruncateException exception) {
    // the error message will include the cause, there is not a nested exception to handle
    return DatabaseException.Code.FAILED_TRUNCATION.get(
        errVars(
            schemaObject,
            exception,
            map -> {
              map.put("cql", CqlPrintUtil.trimmedCql(statement));
              map.put(
                  "values",
                  errFmtJoin(CqlPrintUtil.trimmedPositionalValues(statement), Object::toString));
            }));
  }

  /**
   * Thrown when the coordinator knows there is not enough replicas alive to start a query.
   *
   * <p>This should be retried in the {@link
   * io.stargate.sgv2.jsonapi.service.cqldriver.CqlProxyRetryPolicy} and if we get here then we know
   * the database is really not available.
   *
   * @param exception
   * @return
   */
  @Override
  public RuntimeException handle(UnavailableException exception) {
    return DatabaseException.Code.UNAVAILABLE_DATABASE.get(
        errVars(
            schemaObject,
            exception,
            m -> {
              m.put("requiredNodes", String.valueOf(exception.getRequired()));
              m.put("aliveNodes", String.valueOf(exception.getAlive()));
            }));
  }

  // ========================================================================
  // QueryConsistencyException and subclasses
  // see commend for the QueryExecutionException section
  // ========================================================================

  /**
   * Thrown when there is a non-timeout error while executing the request
   *
   * <p>Each node in the reason map needs to have a reason from the Cassandra class
   * RequestFailureReason:
   *
   * <ul>
   *   <li><code>UNKNOWN</code> (0)
   *   <li><code>READ_TOO_MANY_TOMBSTONES</code> (1)
   *   <li><code>TIMEOUT</code> (2)
   *   <li><code>INCOMPATIBLE_SCHEMA</code> (3)
   *   <li><code>INDEX_NOT_AVAILABLE</code> (4)
   *   <li><code>UNKNOWN_COLUMN</code> (5)
   *   <li><code>UNKNOWN_TABLE</code> (6)
   *   <li><code>REMOTE_STORAGE_FAILURE</code> (7)
   * </ul>
   */
  @Override
  public RuntimeException handle(ReadFailureException exception) {
    var uniqueReasons =
        exception.getReasonMap().values().stream()
            .distinct()
            .map(RequestFailureReason::fromCode)
            .map(Enum::name)
            .toList();

    return DatabaseException.Code.FAILED_READ_REQUEST.get(
        errVars(
            schemaObject,
            exception,
            m -> {
              m.put("cql", CqlPrintUtil.trimmedCql(statement));
              m.put(
                  "values",
                  errFmtJoin(CqlPrintUtil.trimmedPositionalValues(statement), Object::toString));
              m.put("blockForNodes", String.valueOf(exception.getBlockFor()));
              m.put("receivedNodes", String.valueOf(exception.getReceived()));
              m.put("failedNodes", String.valueOf(exception.getNumFailures()));
              m.put("dataPresent", String.valueOf(exception.wasDataPresent()));
              m.put("failureReasons", errFmtJoin(uniqueReasons));
            }));
  }

  @Override
  public RuntimeException handle(ReadTimeoutException exception) {
    return DatabaseException.Code.TIMEOUT_READING_DATA.get(
        errVars(
            schemaObject,
            exception,
            m -> {
              m.put("cql", CqlPrintUtil.trimmedCql(statement));
              m.put(
                  "values",
                  errFmtJoin(CqlPrintUtil.trimmedPositionalValues(statement), Object::toString));
              m.put("blockForNodes", String.valueOf(exception.getBlockFor()));
              m.put("receivedNodes", String.valueOf(exception.getReceived()));
              m.put("dataPresent", String.valueOf(exception.wasDataPresent()));
            }));
  }

  /**
   * Same reasons as the {@link #handle(ReadFailureException)}, but with some different information
   */
  @Override
  public RuntimeException handle(WriteFailureException exception) {
    var uniqueReasons =
        exception.getReasonMap().values().stream()
            .distinct()
            .map(RequestFailureReason::fromCode)
            .map(Enum::name)
            .toList();

    return DatabaseException.Code.FAILED_WRITE_REQUEST.get(
        errVars(
            schemaObject,
            exception,
            m -> {
              m.put("cql", CqlPrintUtil.trimmedCql(statement));
              m.put(
                  "values",
                  errFmtJoin(CqlPrintUtil.trimmedPositionalValues(statement), Object::toString));
              m.put("blockForNodes", String.valueOf(exception.getBlockFor()));
              m.put("receivedNodes", String.valueOf(exception.getReceived()));
              m.put("failedNodes", String.valueOf(exception.getNumFailures()));
              m.put("writeType", String.valueOf(exception.getWriteType()));
              m.put("failureReasons", errFmtJoin(uniqueReasons));
            }));
  }

  @Override
  public RuntimeException handle(WriteTimeoutException exception) {
    return DatabaseException.Code.TIMEOUT_WRITING_DATA.get(
        errVars(
            schemaObject,
            exception,
            m -> {
              m.put("cql", CqlPrintUtil.trimmedCql(statement));
              m.put(
                  "values",
                  errFmtJoin(CqlPrintUtil.trimmedPositionalValues(statement), Object::toString));
              m.put("blockForNodes", String.valueOf(exception.getBlockFor()));
              m.put("receivedNodes", String.valueOf(exception.getReceived()));
              m.put("writeType", String.valueOf(exception.getWriteType()));
            }));
  }

  /**
   * Factory function for creating a new instance of a DriverExceptionHandler.
   *
   * <p>Implementations of the interface should provide a factory function that can be called to
   * create a new instance of the handler. This can just be the constructor for the default class.
   * For example:
   *
   * <pre>
   *   return new GenericOperation<>(attempts, pageBuilder, TableDriverExceptionHandler::new);
   * </pre>
   *
   * See usage of the {@link
   * io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler}
   */
  @FunctionalInterface
  public interface Factory<T extends SchemaObject> {

    /** Signature of the constructor for the DefaultDriverExceptionHandler. */
    DriverExceptionHandler apply(
        RequestContext requestContext,
        T schemaObject,
        SimpleStatement statement,
        CQLSessionCache sessionCache);

    /**
     * Returns a factory that will call {@link FactoryWithIdentifier#apply(RequestContext,
     * SchemaObject, SimpleStatement, CQLSessionCache, CqlIdentifier)} with the supplied identifier.
     */
    static <StaticT extends SchemaObject> Factory<StaticT> withIdentifier(
        FactoryWithIdentifier<StaticT> factoryWithIdentifier, CqlIdentifier identifier) {
      return (requestContext, schemaObject, statement, sessionCache) ->
          factoryWithIdentifier.apply(
              requestContext, schemaObject, statement, sessionCache, identifier);
    }
  }

  /**
   * Most subclasses for command specific exception handlers will need to know the identifier of the
   * object they are working with. This interface provides (sort of) function currying to allow the
   * identifier to be passed and returns the above regular {@link Factory} function.
   *
   * <p>To use this with your customer {@link DefaultDriverExceptionHandler} subclass:
   *
   * <ol>
   *   <li>Declare the constructor with params <code>(SchemaObject, SimpleStatement, CqlIdentifier)
   *       </code> to match {@link FactoryWithIdentifier#apply(RequestContext, SchemaObject,
   *       SimpleStatement, CQLSessionCache, CqlIdentifier)}
   *   <li>Pass a method reference to the constructor to {@link
   *       Factory#withIdentifier(FactoryWithIdentifier, CqlIdentifier)} to get a {@link Factory}
   *       function for example:
   *       <pre>
   *       Factory.withIdentifier(CreateIndexExceptionHandler::new, apiIndex.indexName());
   *     </pre>
   *   <li>Use the result of <code>withIdentifier</code> where you use a factory for example:
   *       <pre>
   *       return new GenericOperation<>(
   *         new OperationAttemptContainer<>(attempt),
   *         pageBuilder,
   *         DefaultDriverExceptionHandler.Factory.withIdentifier(CreateIndexExceptionHandler::new, apiIndex.indexName()));
   *     </pre>
   *       See usage of {@link
   *       io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexExceptionHandler}
   * </ol>
   *
   * @param <T> Type of the schema object the handler is for.
   */
  @FunctionalInterface
  public interface FactoryWithIdentifier<T extends SchemaObject> {

    /** Signature of the constructor for the handler. */
    DriverExceptionHandler apply(
        RequestContext requestContext,
        T schemaObject,
        SimpleStatement statement,
        CQLSessionCache sessionCache,
        CqlIdentifier identifier);
  }
}
