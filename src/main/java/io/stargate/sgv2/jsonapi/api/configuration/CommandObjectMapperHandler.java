package io.stargate.sgv2.jsonapi.api.configuration;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;

public class CommandObjectMapperHandler extends DeserializationProblemHandler {
  @Override
  public JavaType handleUnknownTypeId(
      DeserializationContext ctxt,
      JavaType baseType,
      String subTypeId,
      TypeIdResolver idResolver,
      String failureMsg)
      throws JsonApiException {
    //            interface io.stargate.sgv2.jsonapi.api.model.command.NamespaceCommand ->
    // NamespaceCommand
    //            interface io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand ->
    // CollectionCommand
    //            interface io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand ->
    // GeneralCommand
    final String rawCommandClassString = baseType.getRawClass().toString();
    final String baseCommand =
        rawCommandClassString.substring(rawCommandClassString.lastIndexOf('.') + 1);
    throw new JsonApiException(
        ErrorCode.NO_COMMAND_MATCHED,
        String.format("No '%s' command found as %s", subTypeId, baseCommand));
  }
}
