package io.stargate.sgv2.jsonapi.exception.mappers;


/**
 * Command result supplier for a generic exception.
 *
 * @param t Throwable Exception to map to the {@link CommandResult}.
 */
// public record ThrowableCommandResultSupplier(Throwable t) implements Supplier<CommandResult> {
//
//  /** {@inheritDoc} */
//  @Override
//  public CommandResult get() {
//
//    var builder = CommandResult.statusOnlyBuilder(false, RequestTracing.NO_OP);
//
//    // resolve message
//    builder.addCommandResultError(ThrowableToErrorMapper.getMapperFunction().apply(t));
//    if (t.getCause() != null) {
//
// builder.addCommandResultError(ThrowableToErrorMapper.getMapperFunction().apply(t.getCause()));
//    }
//    return builder.build();
//  }
// }
