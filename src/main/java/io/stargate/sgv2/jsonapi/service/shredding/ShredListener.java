package io.stargate.sgv2.jsonapi.service.shredding;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;

/**
 * Listener API used to decouple details of traversing an input (JSON) document from shredding
 * processing. Callbacks are called in document order when traversing the input document.
 */
public interface ShredListener {
  void shredObject(JsonPath.Builder pathBuilder, ObjectNode obj);

  void shredArray(JsonPath.Builder pathBuilder, ArrayNode arr);

  void shredText(JsonPath path, String text);

  void shredNumber(JsonPath path, BigDecimal number);

  void shredBoolean(JsonPath path, boolean value);

  void shredNull(JsonPath path);
}
