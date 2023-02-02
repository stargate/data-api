package io.stargate.sgv2.jsonapi.exception.mappers;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;

import java.util.List;
import java.util.function.Supplier;

/**
 * Command result supplier for a generic exception.
 *
 * @param t Throwable Exception to map to the {@link CommandResult}.
 */
public record ThrowableCommandResultSupplier(Throwable t) implements Supplier<CommandResult> {

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    // resolve message
    CommandResult.Error error = ThrowableToErrorMapper.getMapperFunction().apply(t);
    if (t.getCause() != null) {
      CommandResult.Error cause = ThrowableToErrorMapper.getMapperFunction().apply(t.getCause());
      return new CommandResult(List.of(error, cause));
    } else {
      return new CommandResult(List.of(error));
    }
  }
}
