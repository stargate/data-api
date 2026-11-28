package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for tests for a {@link CommandResolver} to reduce some boilerplate.
 *
 * <p>Notes for users:
 *
 * <ul>
 *   <li>Godo exmaple of why this exists, see {@link #assertResolver(String, String)}
 *   <li>We still need some CDI injection, so there are abstract properties to access objects
 *       injected after construction on the subclass.
 *   <li>Avoid adding actual @Test tests to this class, as they will be run for all subclasses. Add
 *       features that do asserts etc, and then have the @Test test declared on subclasses
 * </ul>
 *
 * @param <SCHEMA> the type of the schema under test, this is the type used by the operation created
 * @param <COMMAND> the type of the command under test
 * @param <RESOLVER> the type of the resolver we will pass the command for testing
 * @param <OPERATION> the type of the operation that should be created by the resolver.
 */
abstract class CommandResolverTestBase<
    SCHEMA extends SchemaObject,
    COMMAND extends Command,
    RESOLVER extends CommandResolver<COMMAND>,
    OPERATION extends Operation<SCHEMA>> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(CommandResolverTestBase.class);

  protected final TestConstants TEST_CONSTANTS = new TestConstants();

  // =================================================================
  // Required implementation for subclasses
  // =================================================================

  /**
   * Implementations should return an instance of the resolver for test, may be a new instance every
   * call or same instance.
   */
  protected abstract RESOLVER resolver();

  /**
   * Implementations should return the CommandContext to call the resolver with, this may be
   * different for each call but recommend it is the same.
   */
  protected abstract CommandContext<SCHEMA> commandContext();

  /** Class of {@link COMMAND}, the JSON of the command is deseralised into this class. */
  protected abstract Class<COMMAND> commandClass();

  // =================================================================
  // Useful assertions for subclasses to use.
  // =================================================================

  /**
   * Resolves the command described in the rawJson and does basic assertions on the operaton
   * returned. Most subclasses testing a resolve need this.
   *
   * @param testName name for logging etc
   * @param rawJson the raw command JSON to parse into a command, {@link
   *     TestConstants#rawNamesSubstitutor()} is used to replace names such as <code>${collection}
   *     </code>
   * @return The operation the resolver created.
   */
  protected OPERATION assertResolver(String testName, String rawJson) {

    var json = TEST_CONSTANTS.subsRawNames(rawJson);
    LOGGER.info("assertResolver() - testName: {}, (substituted) json: {}", testName, json);

    COMMAND command;
    try {
      command = TEST_CONSTANTS.OBJECT_MAPPER.readValue(json, commandClass());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    var localCommandContext = commandContext();
    var operation = resolver().resolveCommand(localCommandContext, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            CreateCollectionOperation.class,
            op -> {
              assertThat(op.commandContext())
                  .as("%s - commandContext() is same object", testName)
                  .isSameAs(localCommandContext);
            });
    return (OPERATION) operation;
  }

  /**
   * Runs the command through the resolver, and assumes it will throw an exception.
   *
   * <p>See {@link #assertResolver(String, String)}
   */
  protected Throwable assertResolverThrows(String testName, String rawJson) {
    var throwable = catchThrowable(() -> assertResolver(testName, rawJson));

    assertThat(throwable).as("%s - exception is thrown", testName).isNotNull();

    return throwable;
  }
}
