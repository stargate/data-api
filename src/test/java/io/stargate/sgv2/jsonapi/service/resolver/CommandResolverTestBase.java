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
 *   <li>We still need some CDI injection, so there are abstract properties to access objects
 *       injected after construction on the subclass.
 * </ul>
 *
 * @param <SCHEMA> the type of the schema under test, this is the type used by the opertion created
 * @param <COMMAND> the type of the command under test
 * @param <RESOLVER> the type of the resolver under test
 */
abstract class CommandResolverTestBase<
    SCHEMA extends SchemaObject,
    COMMAND extends Command,
    RESOLVER extends CommandResolver<COMMAND>,
    OPERATION extends Operation<SCHEMA>> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(CommandResolverTestBase.class);

  protected final TestConstants TEST_CONSTANTS = new TestConstants();

  protected abstract RESOLVER resolver();

  protected abstract CommandContext<SCHEMA> commandContext();

  protected abstract Class<COMMAND> commandClass();

  protected Throwable assertResolverThrows(String testName, String rawJson) {
    var throwable = catchThrowable(() -> assertResolver(testName, rawJson));

    assertThat(throwable).as("%s - exception is thrown", testName).isNotNull();

    return throwable;
  }

  /**
   * Run the command through the resolver and do some basic tests on the operation that comes out.
   *
   * @param testName name for logging etc
   * @param rawJson the raw command JSON to parse into a command, {@link
   *     TestConstants#rawNamesSubstitutor()} is used to replace names
   * @return The operation the resolver created.
   */
  protected OPERATION assertResolver(String testName, String rawJson) {

    var json = TEST_CONSTANTS.subsRawNames(rawJson);
    LOGGER.info("assertResolver() - testName: {}, json: {}", testName, json);

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
}
