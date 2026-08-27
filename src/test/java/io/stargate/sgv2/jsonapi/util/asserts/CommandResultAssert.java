package io.stargate.sgv2.jsonapi.util.asserts;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import org.assertj.core.api.AbstractAssert;

/**
 * Assertions for matching the {@link CommandResult}
 *
 * <p>NOTE: there is some logic duplication with {@link ResponseAssertions} which is used for
 * integration tests which tie in with RestEasy. It would be good to have some underlying logic but
 * that can be future refactoring
 */
public class CommandResultAssert extends AbstractAssert<CommandResultAssert, CommandResult> {

  private CommandResultAssert(CommandResult commandResult) {
    super(commandResult, CommandResultAssert.class);
  }

  protected static CommandResultAssert assertThatCommandResult(CommandResult commandResult) {

    // must always be non null
    assertThat(commandResult).as("CommandResult is not null").isNotNull();
    return new CommandResultAssert(commandResult);
  }

  public CommandResultAssert isDDLSuccess() {

    assertThat(actual.data()).as("isDDLSuccess() - data is null").isNull();
    assertThat(actual.status()).as("isError() - status is no empty").isNotEmpty();
    assertThat(actual.errors()).as("isDDLSuccess() - errors is empty").isEmpty();
    return this;
  }

  public CommandResultAssert isError() {

    assertThat(actual.data()).as("isError() - data is null").isNull();
    assertThat(actual.status()).as("isError() - status is empty").isEmpty();
    assertThat(actual.errors()).as("isError() - errors is not null").isNotNull();
    assertThat(actual.errors()).as("isError() - errors is not empty").isNotEmpty();
    return this;
  }

  public CommandResultAssert hasOnlyStatus(CommandStatus key, Object value) {
    assertThat(actual.status()).as("status is not null").isNotNull();
    assertThat(actual.status()).as("status is only one").hasSize(1);
    assertThat(actual.status().get(key)).as("status['%s']", key).isEqualTo(value);

    return this;
  }

  public CommandResultAssert hasNoStatus() {
    assertThat(actual.status()).as("status is empty").isEmpty();
    return this;
  }

  public CommandResultAssert hasNoErrors() {
    assertThat(actual.errors()).as("errors is empty").isEmpty();
    return this;
  }

  public CommandResultAssert hasErrorCount(int count) {
    assertThat(actual.errors()).as("errors is not null").isNotNull();
    assertThat(actual.errors()).as("errors size").hasSize(count);
    return this;
  }

  public CommandResultAssert hasOnlyError(ErrorCode<?> errorCode, String... snippets) {

    assertThat(actual.errors()).as("errors is not null").isNotNull();
    assertThat(actual.errors()).as("errors size").hasSize(1);

    var commandError = actual.errors().getFirst();
    assertThat(commandError.errorCode()).as("errorCode").isEqualTo(errorCode.name());

    for (var snippet : snippets) {
      assertThat(commandError.message()).as("message").contains(snippet);
    }
    return this;
  }
}
