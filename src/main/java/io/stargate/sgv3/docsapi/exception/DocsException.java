package io.stargate.sgv3.docsapi.exception;

import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Our own {@link RuntimeException} that uses {@link ErrorCode} to describe the exception cause.
 * Supports specification of the custom message.
 *
 * <p>Implements {@link Supplier<CommandResult>} so this exception can be mapped to command result
 * directly.
 */
public class DocsException extends RuntimeException implements Supplier<CommandResult> {

  private final ErrorCode errorCode;

  // TODO do we need a cause support, we could create two errors then in the list :)

  public DocsException(ErrorCode errorCode) {
    this.errorCode = errorCode;
  }

  public DocsException(ErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    return toCommandResult(this);
  }

  /**
   * Public util method for translating a {@link DocsException} to {@link CommandResult}.
   *
   * @param e Exception
   * @return CommandResult
   */
  public static CommandResult toCommandResult(DocsException e) {
    // resolve message
    String message = e.getMessage();
    if (message == null) {
      message = e.errorCode.getMessage();
    }

    // add error code as error field
    Map<String, Object> fields = Map.of("errorCode", e.errorCode.name());

    // construct and return
    CommandResult.Error error = new CommandResult.Error(message, fields);
    return new CommandResult(List.of(error));
  }
}
