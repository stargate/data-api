package io.stargate.sgv2.jsonapi.service.shredding.model;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

/**
 * Value that represents details of MongoDB {@code _id} column: the primary key of all Documents
 * stored in Collections. Its type can be one of 4 native Mongo types:
 *
 * <ul>
 *   <li>String
 *   <li>Number
 *   <li>Boolean
 *   <li>null
 * </ul>
 */
@RegisterForReflection
public interface DocumentId {
  int typeId();

  @JsonValue
  Object value();

  default JsonNode asJson(ObjectMapper mapper) {
    return asJson(mapper.getNodeFactory());
  }

  JsonNode asJson(JsonNodeFactory nodeFactory);

  static DocumentId fromJson(JsonNode node) {
    switch (node.getNodeType()) {
      case BOOLEAN -> {
        return fromBoolean(node.booleanValue());
      }
      case NULL -> {
        return fromNull();
      }
      case NUMBER -> {
        return fromNumber(node.decimalValue());
      }
      case STRING -> {
        return fromString(node.textValue());
      }
    }
    throw new JsonApiException(
        ErrorCode.SHRED_BAD_DOCID_TYPE,
        String.format(
            "%s: Document Id must be a JSON String, Number, Boolean or NULL instead got %s",
            ErrorCode.SHRED_BAD_DOCID_TYPE.getMessage(), node.getNodeType()));
  }

  static DocumentId fromDatabase(int typeId, String documentIdAsText) {
    switch (DocumentConstants.KeyTypeId.getJsonType(typeId)) {
      case BOOLEAN -> {
        return fromBoolean(Boolean.valueOf(documentIdAsText));
      }
      case NULL -> {
        return fromNull();
      }
      case NUMBER -> {
        return fromNumber(new BigDecimal(documentIdAsText));
      }
      case STRING -> {
        return fromString(documentIdAsText);
      }
    }
    throw new JsonApiException(
        ErrorCode.SHRED_BAD_DOCID_TYPE,
        String.format(
            "%s: Document Id must be a JSON String(1), Number(2), Boolean(3) or NULL(4) instead got %s",
            ErrorCode.SHRED_BAD_DOCID_TYPE.getMessage(), typeId));
  }

  static DocumentId fromBoolean(boolean key) {
    return BooleanId.valueOf(key);
  }

  static DocumentId fromNull() {
    return NullId.NULL;
  }

  static DocumentId fromNumber(BigDecimal key) {
    key = Objects.requireNonNull(key);
    return new NumberId(key);
  }

  static DocumentId fromString(String key) {
    key = Objects.requireNonNull(key);
    if (key.isEmpty()) {
      throw new JsonApiException(ErrorCode.SHRED_BAD_DOCID_EMPTY_STRING);
    }
    return new StringId(key);
  }

  static DocumentId fromUUID(UUID uuid) {
    return new StringId(uuid.toString());
  }

  /*
  /**********************************************************************
  /* Concrete implementation types
  /**********************************************************************
   */

  record StringId(String key) implements DocumentId {
    @Override
    public int typeId() {
      return DocumentConstants.KeyTypeId.TYPE_ID_STRING;
    }

    @Override
    public Object value() {
      return key();
    }

    @Override
    public JsonNode asJson(JsonNodeFactory nodeFactory) {
      return nodeFactory.textNode(key);
    }

    @Override
    public String toString() {
      return key;
    }
  }

  record NumberId(BigDecimal key) implements DocumentId {
    @Override
    public int typeId() {
      return DocumentConstants.KeyTypeId.TYPE_ID_NUMBER;
    }

    @Override
    public Object value() {
      return key();
    }

    @Override
    public JsonNode asJson(JsonNodeFactory nodeFactory) {
      return nodeFactory.numberNode(key);
    }

    @Override
    public String toString() {
      return String.valueOf(key);
    }
  }

  record BooleanId(boolean key) implements DocumentId {
    private static final BooleanId FALSE = new BooleanId(false);
    private static final BooleanId TRUE = new BooleanId(true);

    public static BooleanId valueOf(boolean b) {
      return b ? TRUE : FALSE;
    }

    @Override
    public int typeId() {
      return DocumentConstants.KeyTypeId.TYPE_ID_BOOLEAN;
    }

    @Override
    public Object value() {
      return key();
    }

    @Override
    public JsonNode asJson(JsonNodeFactory nodeFactory) {
      return nodeFactory.booleanNode(key);
    }

    @Override
    public String toString() {
      return String.valueOf(key);
    }
  }

  record NullId() implements DocumentId {
    public static final NullId NULL = new NullId();

    @Override
    public Object value() {
      return null;
    }

    @Override
    public int typeId() {
      return DocumentConstants.KeyTypeId.TYPE_ID_NULL;
    }

    @Override
    public JsonNode asJson(JsonNodeFactory nodeFactory) {
      return nodeFactory.nullNode();
    }

    @Override
    public String toString() {
      return "null";
    }
  }
}
