package io.stargate.sgv2.jsonapi.service.shredding.model;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

/**
 * Value that represents details of MongoDB {@code _id} column: the primary key of all Documents
 * stored in Collections. Its type can be one of 5 native Mongo types:
 *
 * <ul>
 *   <li>String
 *   <li>Number
 *   <li>Boolean
 *   <li>null
 *   <li>Date (EJSON-encoded)
 * </ul>
 */
@RegisterForReflection
public interface DocumentId {
  int typeId();

  /** Method called by JSON serializer to get value to include in JSON output. */
  @JsonValue
  Object value();

  default JsonNode asJson(ObjectMapper mapper) {
    return asJson(mapper.getNodeFactory());
  }

  JsonNode asJson(JsonNodeFactory nodeFactory);

  /**
   * Accessor used to get canonical String representation of the id to be stored in database. Does
   * NOT contain type prefix or suffix.
   *
   * @return Canonical String representation of the id
   */
  String asDBKey();

  static DocumentId fromJson(JsonNode node) {
    switch (node.getNodeType()) {
      case BOOLEAN:
        return fromBoolean(node.booleanValue());
      case NULL:
        return fromNull();
      case NUMBER:
        return fromNumber(node.decimalValue());
      case STRING:
        return fromString(node.textValue());
      case OBJECT:
        if (!JsonUtil.looksLikeEJsonValue(node)) {
          break;
        }
        JsonExtensionType extType = JsonUtil.findJsonExtensionType(node);
        if (extType != null) {
          // We know it's single-entry Object so can just get the one property value
          JsonNode valueNode = node.iterator().next();
          switch (extType) {
            case EJSON_DATE:
              if (valueNode.isIntegralNumber() && valueNode.canConvertToLong()) {
                return fromTimestamp(valueNode.longValue());
              }
              break;
            default:
              return fromExtensionType(extType, valueNode);
          }
        }
        throw ErrorCode.SHRED_BAD_DOCID_TYPE.toApiException(
            "unrecognized JSON extension type '%s'", node.fieldNames().next());
    }
    throw ErrorCode.SHRED_BAD_DOCID_TYPE.toApiException(
        "Document Id must be a JSON String, Number, Boolean, EJSON-Encoded Date Object or NULL instead got %s",
        node.getNodeType());
  }

  static DocumentId fromDatabase(int typeId, String documentIdAsText) {
    JsonType type = DocumentConstants.KeyTypeId.getJsonType(typeId);
    if (type == null) {
      throw ErrorCode.SHRED_BAD_DOCID_TYPE.toApiException(
          "Document Id must be a JSON String(1), Number(2), Boolean(3), NULL(4) or Date(5) instead got %d",
          typeId);
    }
    switch (type) {
      case BOOLEAN -> {
        switch (documentIdAsText) {
          case "true":
            return fromBoolean(true);
          case "false":
            return fromBoolean(false);
        }
        throw new JsonApiException(
            ErrorCode.SHRED_BAD_DOCID_TYPE,
            String.format(
                "%s: Document Id type Boolean stored as invalid String '%s' (must be 'true' or 'false')",
                ErrorCode.SHRED_BAD_DOCID_TYPE.getMessage(), documentIdAsText));
      }
      case NULL -> {
        return fromNull();
      }
      case NUMBER -> {
        try {
          return fromNumber(new BigDecimal(documentIdAsText));
        } catch (NumberFormatException e) {
          throw new JsonApiException(
              ErrorCode.SHRED_BAD_DOCID_TYPE,
              String.format(
                  "%s: Document Id type Number stored as invalid String '%s' (not a valid Number)",
                  ErrorCode.SHRED_BAD_DOCID_TYPE.getMessage(), documentIdAsText));
        }
      }
      case STRING -> {
        return fromString(documentIdAsText);
      }
      case DATE -> {
        try {
          long ts = Long.parseLong(documentIdAsText);
          return fromTimestamp(ts);
        } catch (NumberFormatException e) {
          throw new JsonApiException(
              ErrorCode.SHRED_BAD_DOCID_TYPE,
              String.format(
                  "%s: Document Id type Date stored as invalid String '%s' (needs to be Number)",
                  ErrorCode.SHRED_BAD_DOCID_TYPE.getMessage(), documentIdAsText));
        }
      }
    }
    throw new JsonApiException(ErrorCode.SHRED_BAD_DOCID_TYPE);
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

  static DocumentId fromTimestamp(Date key) {
    return new DateId(key.getTime());
  }

  static DocumentId fromTimestamp(long keyAsLong) {
    return new DateId(keyAsLong);
  }

  static DocumentId fromExtensionType(JsonExtensionType extType, JsonNode valueNode) {
    try {
      Object rawId = JsonUtil.extractExtendedValueUnwrapped(extType, valueNode);
      return new ExtensionTypeId(extType, String.valueOf(rawId));
    } catch (JsonApiException e) {
      throw ErrorCode.SHRED_BAD_DOCID_TYPE.toApiException(e.getMessage());
    }
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
    public String asDBKey() {
      return key();
    }

    @Override
    public String toString() {
      // Enclose in single-quotes to indicate it is String value (not to overlap
      // with Number, Boolean, null values), indicate start/end
      // TODO: Consider escaping of quotes within value?
      return "'" + key + "'";
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
    public String asDBKey() {
      return String.valueOf(key);
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
    public String asDBKey() {
      return String.valueOf(key);
    }

    @Override
    public String toString() {
      return String.valueOf(key);
    }
  }

  record DateId(long key) implements DocumentId {
    @Override
    public int typeId() {
      return DocumentConstants.KeyTypeId.TYPE_ID_DATE;
    }

    @Override
    public Object value() {
      // Important! Need to serialize as EJSON Encoded representation for use by Jackson
      return JsonUtil.createEJSonDateAsMap(key());
    }

    @Override
    public JsonNode asJson(JsonNodeFactory nodeFactory) {
      return JsonUtil.createEJSonDate(nodeFactory, key);
    }

    @Override
    public String asDBKey() {
      return String.valueOf(key());
    }

    @Override
    public String toString() {
      return asDBKey();
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
    public String asDBKey() {
      return "null";
    }

    @Override
    public String toString() {
      return "null";
    }
  }

  record ExtensionTypeId(JsonExtensionType type, String valueAsString) implements DocumentId {
    @Override
    public int typeId() {
      return DocumentConstants.KeyTypeId.TYPE_ID_STRING;
    }

    @Override
    public Object value() {
      // Important! Need to serialize as JSON Extension representation for use by Jackson
      return JsonUtil.createJsonExtensionValueAsMap(type(), valueAsString());
    }

    @Override
    public JsonNode asJson(JsonNodeFactory nodeFactory) {
      // Stored as JSON Object in doc_json, needs to be exposed as JSON Object
      // here as well
      return JsonUtil.createJsonExtensionValue(nodeFactory, type(), valueAsString());
    }

    @Override
    public String asDBKey() {
      return valueAsString();
    }

    @Override
    public String toString() {
      return valueAsString();
    }
  }
}
