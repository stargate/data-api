package io.stargate.sgv2.jsonapi.exception.mappers;


/**
 * Translates any exception that is not handled by the command engine during the API operation to a
 * CommandResult, with status `HTTP 200`.
 */
// public class GenericExceptionMapper {
//
//  // explicitly add types to override Quarkus mappers
//  @ServerExceptionMapper({
//    Exception.class,
//    JsonParseException.class,
//    MismatchedInputException.class
//  })
//  public RestResponse<CommandResult> genericExceptionMapper(Throwable e) {
//
//    CommandResult commandResult = new ThrowableCommandResultSupplier(e).get();
//    return commandResult.toRestResponse();
//  }
// }
