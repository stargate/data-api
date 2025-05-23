package io.stargate.sgv2.jsonapi.api.configuration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;

public class CommandObjectMapperHandler extends DeserializationProblemHandler {

  @Override
  public boolean handleUnknownProperty(
      DeserializationContext ctxt,
      JsonParser p,
      JsonDeserializer<?> deserializer,
      Object beanOrClass,
      String propertyName) {
    // First: handle known/observed CreateCollectionCommand mapping discrepancies

    final String typeStr = (deserializer == null) ? "N/A" : deserializer.handledType().toString();
    if (typeStr.endsWith("CreateCollectionCommand$Options")) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "No option \"%s\" exists for `createCollection.options` (valid options: \"defaultId\", \"indexing\", \"lexical\", \"rerank\", \"vector\")",
          propertyName);
    }
    if (typeStr.endsWith("CreateCollectionCommand$Options$IdConfig")) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Unrecognized field \"%s\" for `createCollection.options.defaultId` (known fields: \"type\")",
          propertyName);
    }
    if (typeStr.endsWith("CreateCollectionCommand$Options$IndexingConfig")) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Unrecognized field \"%s\" for `createCollection.options.indexing` (known fields: \"allow\", \"deny\")",
          propertyName);
    }
    if (typeStr.endsWith("CreateCollectionCommand$Options$VectorSearchConfig")) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "Unrecognized field \"%s\" for `createCollection.options.vector` (known fields: \"dimension\", \"metric\", \"service\", \"sourceModel\",)",
          propertyName);
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
      String failureMsg)
      throws JsonApiException {
    final String rawCommandClassString = baseType.getRawClass().getName();
    final String baseCommand =
        rawCommandClassString.substring(rawCommandClassString.lastIndexOf('.') + 1);
    throw ErrorCodeV1.COMMAND_UNKNOWN.toApiException(
        "\"%s\" not one of \"%s\"s: known commands are %s",
        subTypeId, baseCommand, idResolver.getDescForKnownTypeIds());
  }
}
