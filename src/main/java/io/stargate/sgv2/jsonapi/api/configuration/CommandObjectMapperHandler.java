package io.stargate.sgv2.jsonapi.api.configuration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import java.util.Map;

public class CommandObjectMapperHandler extends DeserializationProblemHandler {

  @Override
  public boolean handleUnknownProperty(
      DeserializationContext ctxt,
      JsonParser p,
      JsonDeserializer<?> deserializer,
      Object beanOrClass,
      String propertyName) {
    // First: handle known/observed CreateCollectionCommand mapping discrepancies

    if (CreateCollectionCommand.Options.class.equals(deserializer.handledType())) {
      throw RequestException.Code.INVALID_CREATE_COLLECTION_FIELD.get(
          "message",
          "No option \"%s\" exists for `createCollection.options` (valid options: \"defaultId\", \"indexing\", \"lexical\", \"rerank\", \"vector\")"
              .formatted(propertyName));
    }

    if (CreateCollectionCommand.Options.DocIdDesc.class.equals(deserializer.handledType())) {
      throw RequestException.Code.INVALID_CREATE_COLLECTION_FIELD.get(
          "message",
          "Unrecognized field \"%s\" for `createCollection.options.defaultId` (known fields: \"type\")"
              .formatted(propertyName));
    }
    if (CreateCollectionCommand.Options.IndexingDesc.class.equals(deserializer.handledType())) {
      throw RequestException.Code.INVALID_CREATE_COLLECTION_FIELD.get(
          "message",
          "Unrecognized field \"%s\" for `createCollection.options.indexing` (known fields: \"allow\", \"deny\")"
              .formatted(propertyName));
    }
    if (CreateCollectionCommand.Options.VectorSearchDesc.class.equals(deserializer.handledType())) {
      throw RequestException.Code.INVALID_CREATE_COLLECTION_FIELD.get(
          "message",
          "Unrecognized field \"%s\" for `createCollection.options.vector` (known fields: \"dimension\", \"metric\", \"service\", \"sourceModel\")"
              .formatted(propertyName));
    }

    // false means if not matched by above handle logic, object mapper will
    // FAIL_ON_UNKNOWN_PROPERTIES -- but may be re-mapped by ThrowableToErrorMapper
    return false;
  }

  @Override
  public JavaType handleUnknownTypeId(
      DeserializationContext ctxt,
      JavaType baseType,
      String subTypeId,
      TypeIdResolver idResolver,
      String failureMsg) {

    var rawCommandClassString = baseType.getRawClass().getName();
    var baseCommand = rawCommandClassString.substring(rawCommandClassString.lastIndexOf('.') + 1);

    // Massage "GeneralCommand" into "General Command" (and so forth)
    int ix = baseCommand.indexOf("Command");
    if (ix > 0) {
      baseCommand = baseCommand.substring(0, ix) + " " + "Command";
    } else {
      // Also handle nested polymorphic operations like "AlterCollectionOperation" ->
      // "AlterCollection Operation" so the error message reads more naturally.
      int opIx = baseCommand.indexOf("Operation");
      if (opIx > 0) {
        baseCommand = baseCommand.substring(0, opIx) + " " + "Operation";
      }
    }

    throw RequestException.Code.COMMAND_UNKNOWN.get(
        Map.of(
            "commandType",
            baseCommand,
            "command",
            subTypeId,
            "knownCommands",
            idResolver.getDescForKnownTypeIds()));
  }
}
