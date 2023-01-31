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
public abstract class DocumentId {
  protected final JsonType type;

  protected DocumentId(JsonType type) {
    this.type = type;
  }

  public JsonType type() {
    return type;
  }

  public JsonNode asJson(ObjectMapper mapper) {
    return asJson(mapper.getNodeFactory());
  }

  public abstract JsonNode asJson(JsonNodeFactory nodeFactory);

  /** Overridden as abstract to force sub-classes to re-implement */
  @Override
  public abstract String toString();

  public static DocumentId fromJson(JsonNode node) {
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

  public static class StringId extends DocumentId {
    private final String key;

    public StringId(String key) {
      super(JsonType.STRING);
      this.key = key;
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

  public static class NumberId extends DocumentId {
    private final BigDecimal key;

    public NumberId(BigDecimal key) {
      super(JsonType.NUMBER);
      this.key = key;
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

  public static class BooleanId extends DocumentId {
    private final boolean key;

    private static final BooleanId FALSE = new BooleanId(false);
    private static final BooleanId TRUE = new BooleanId(true);

    private BooleanId(boolean key) {
      super(JsonType.BOOLEAN);
      this.key = key;
    }

    public static BooleanId valueOf(boolean b) {
      return b ? TRUE : FALSE;
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

  public static class NullId extends DocumentId {
    public static final NullId NULL = new NullId();

    private NullId() {
      super(JsonType.NULL);
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
