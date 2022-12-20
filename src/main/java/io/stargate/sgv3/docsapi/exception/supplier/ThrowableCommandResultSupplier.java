package io.stargate.sgv3.docsapi.exception.supplier;

import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Command result supplier for a generic exception.
 *
 * @param t Throwable
 */
public record ThrowableCommandResultSupplier(Throwable t) implements Supplier<CommandResult> {

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    return toCommandResult(t);
  }

  /**
   * Static utility to transform any throwable to CommandResult.
   *
   * @param t Throwable
   * @return CommandResult
   */
  public static CommandResult toCommandResult(Throwable t) {
    return toCommandResult(t, true);
  }

  /**
   * Static utility to transform any throwable to CommandResult.
   *
   * @param t Throwable
   * @param includeCause If cause of the throwable should also be included in the list of errors.
   * @return CommandResult
   */
  public static CommandResult toCommandResult(Throwable t, boolean includeCause) {
    // resolve message
    CommandResult.Error error = getError(t);
    if (includeCause && t.getCause() != null) {
      CommandResult.Error cause = getError(t.getCause());
      return new CommandResult(List.of(error, cause));
    } else {
      return new CommandResult(List.of(error));
    }
  }

  /**
   * Gets a {@link CommandResult.Error} for a single Throwable.
   *
   * @param t Throwable
   * @return CommandResult
   */
  public static CommandResult.Error getError(Throwable t) {
    String message = t.getMessage();
    if (message == null) {
      message = "Unexpected exception occurred.";
    }

    // add error code as error field
    Map<String, Object> fields = Map.of("exceptionClass", t.getClass().getSimpleName());
    return new CommandResult.Error(message, fields);
  }
}
