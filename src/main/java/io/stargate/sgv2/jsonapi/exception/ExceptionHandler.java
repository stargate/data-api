package io.stargate.sgv2.jsonapi.exception;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/**
 * Interface for handling <code>RuntimeException</code> and turning them into something else,
 * normally a {@link APIException}
 *
 * <p>The interface sets our a basic contract for handling exceptions of a specific type, and
 * potentially it's subclasses. The first use for this is the {@link
 * io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler} and the basics have
 * been extracted into this super interface because there will be more of these such as how we
 * handle exceptions from Embedding Providers and the Rest framework we run in.
 *
 * <p>The pattern this interface and it's implementations are supporting is:
 *
 * <ol>
 *   <li>Handle errors as close to where they are thrown as possible, and translate them into the
 *       appropriate {@link APIException} to return for the request.
 *   <li>When handling an exception, be aware of the schema type so that errors can be as specific
 *       as possible.
 *   <li>To support this, provide an interface so that code that needs to catch and handle errors
 *       can be injected with the handling "strategy" given the context. For example: we want to
 *       handle insert and delete on collections differently, because the delete has a read first
 *       which may be confusing to get a read timeout. And we may want to handle an insert on a
 *       table differently to an insert on a collection to give diff error codes.
 * </ol>
 *
 * <p>Recommended approach to using this interface see example {@link
 * io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler}:
 *
 * <ul>
 *   <li>Create an interface that implements the <code>handle()</code> for the <code>T</code>
 *       and downcasts to subclasses, then defines <code>handle()</code> overrides for the sub
 *       classes that have a default that returns the exception unchanged.
 *   <li>Create a "Default" class implementation of the interface that implements the <code>handle()
 *       </code> functions. This approach is recommended so that all the "plumbing" of downcasting
 *       etc is kept away the "business logic" of how to handle the exceptions.
 * </ul>
 *
 * <p>Users of the interface shoudl call {@link #maybeHandle(SchemaObject, RuntimeException)} and
 * then throw or otherwise deal with the object returned, which wil be the original exception or a
 * new one. Example:
 *
 * <pre>
 *
 *   public void someMethod(TableSchemaObject table, ExceptionHandler<TableSchemaObject, DriverException> handler) {
 *    try {
 *      // do something that may throw an exception
 *    } catch (DriverException e) {
 *    throw handler.maybeHandle(table, e);
 *   }
 * </pre>
 *
 * <p>Implementations should override the handle() functions for the errors they care about. The
 * default is for the <code>handle()</code> function to return the object unchanged. If an exception
 * is not changed to a different object then {@link #maybeHandle(SchemaObject, RuntimeException)}
 * will call {@link #handleUnhandled(SchemaObject, RuntimeException)} as a last chance to change the
 * driver exception into something else.
 *
 * <p><b>NOTE:</b> Subclass {@link DefaultDriverExceptionHandler} rather than implement this
 * interface directly.
 *
 * @param <T> The base type of the exception that this handler handles, e.g. <code>
 *     DriverException</code>
 */
public interface ExceptionHandler<T extends RuntimeException> {

  /**
   * Handles the <code>runtimeException</code> returning an exception that can be thrown or
   * otherwise handled.
   *
   * @param runtimeException The exception to handle
   * @return The exception to throw or otherwise handle, may be:
   *     <ul>
   *       <li><code>null</code> if null passed in
   *       <li>The exact <code>runtimeException</code> object if it is not an instance of <code>
   *           T</code> using {@link Class#isInstance(Object)}
   *       <li>The handled exception, usuaully translated into an {@link APIException}
   *       <li>If not specifially handler, the exception returned from {@link
   *           #handleUnhandled(SchemaObject, RuntimeException)}
   *     </ul>
   */
  default RuntimeException maybeHandle(RuntimeException runtimeException) {

    if (getExceptionClass().isInstance(runtimeException)) {
      T t = getExceptionClass().cast(runtimeException);
      var handled = handle(t);
      return handled == runtimeException ? handleUnhandled(t) : handled;
    }
    return runtimeException;
  }

  /**
   * Implementations must return the class of the exception they handle, the class of <code>T
   * </code>
   *
   * @return The class of the exception that this handler handles, e.g. <code>DriverException.class
   *     </code>
   */
  Class<T> getExceptionClass();

  /**
   * Called to handle the exception, when it is the same class as <code>T</code>.
   *
   * <p>See class description for full responsibilities of this function.
   *
   * @param exception The exception to handle, of type <code>T</code>
   * @return The exception passed in, or a new exception to throw or otherwise handle.
   */
  default RuntimeException handle(T exception) {
    return exception;
  }

  /**
   * Called by {@link #maybeHandle(RuntimeException)} when an exception was not
   * changed by any handler functions.
   *
   * @param exception The exception that was not handled.
   * @return A {@link ServerException.Code#UNEXPECTED_SERVER_ERROR}.
   */
  default RuntimeException handleUnhandled(T exception) {
    return ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(exception));
  }
}
