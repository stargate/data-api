package io.stargate.sgv2.jsonapi.api.configuration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.io.IOException;

public class CommandObjectMapperHandler extends DeserializationProblemHandler {

  @Override
  public boolean handleUnknownProperty(
      DeserializationContext ctxt,
      JsonParser p,
      JsonDeserializer<?> deserializer,
      Object beanOrClass,
      String propertyName)
      throws IOException {
    if (deserializer.handledType().toString().endsWith("CreateCollectionCommand$Options")) {
      throw ErrorCode.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "No option \"%s\" found as createCollectionCommand option (known options: \"indexing\", \"vector\")",
          propertyName);
    }
    // false means if not matched by above handle logic, object mapper will
    // FAIL_ON_UNKNOWN_PROPERTIES.
    return false;
  }

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
        String.format("No \"%s\" command found as \"%s\"", subTypeId, baseCommand));
  }
}
