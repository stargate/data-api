package io.stargate.sgv2.jsonapi.service.shredding.collections;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;

/**
 * Listener API used to decouple details of traversing an input (JSON) document from shredding
 * processing. Callbacks are called in document order when traversing the input document.
 */
public interface DocumentShredderListener {
  /**
   * @return Whether to traverse contents or not: if {@code true} traverse contents, if {@code
   *     false} do not proceed further.
   */
  boolean shredObject(JsonPath path, ObjectNode obj);

  void shredArray(JsonPath path, ArrayNode arr);

  void shredText(JsonPath path, String text);

  void shredNumber(JsonPath path, BigDecimal number);

  void shredBoolean(JsonPath path, boolean value);

  void shredNull(JsonPath path);

  void shredLexical(JsonPath path, String content);

  void shredVector(JsonPath path, ArrayNode vector);

  void shredVector(JsonPath path, byte[] binaryVector);

  void shredVectorize(JsonPath path);
}
