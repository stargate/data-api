package io.stargate.sgv3.docsapi.service.shredding.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import java.math.BigDecimal;
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
public interface DocumentId {
  JsonType type();

  public default JsonNode asJson(ObjectMapper mapper) {
    return asJson(mapper.getNodeFactory());
  }

  JsonNode asJson(JsonNodeFactory nodeFactory);

  static DocumentId fromJson(JsonNode node) {
    switch (node.getNodeType()) {
      case BOOLEAN -> {
        return BooleanId.valueOf(node.booleanValue());
      }
      case NULL -> {
        return NullId.NULL;
      }
      case NUMBER -> {
        return new NumberId(node.decimalValue());
      }
      case STRING -> {
        return new StringId(node.textValue());
      }
    }
    throw new DocsException(
        ErrorCode.SHRED_BAD_DOCID_TYPE,
        String.format(
            "%s: Document Id must be a JSON String, Number, Boolean or NULL instead got %s",
            ErrorCode.SHRED_BAD_DOCID_TYPE.getMessage(), node.getNodeType()));
  }

  public static DocumentId fromUUID(UUID uuid) {
    return new StringId(uuid.toString());
  }

  /*
  /**********************************************************************
  /* Concrete implementation types
  /**********************************************************************
   */

  public static class StringId implements DocumentId {
    private final String key;

    public StringId(String key) {
      this.key = key;
    }

    @Override
    public JsonType type() {
      return JsonType.STRING;
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
    public JsonType type() {
      return JsonType.NUMBER;
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
    public JsonType type() {
      return JsonType.BOOLEAN;
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
    public JsonType type() {
      return JsonType.NULL;
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
