package io.stargate.sgv3.docsapi.service.shredding;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;

/**
 * Listener API used to decouple details of traversing an input (JSON) document from shredding
 * processing. Callbacks are called in document order when traversing the input document.
 */
public interface ShredListener {
  void shredObject(JSONPath.Builder pathBuilder, ObjectNode obj);

  void shredArray(JSONPath.Builder pathBuilder, ArrayNode arr);

  void shredText(JSONPath path, String text);

  void shredNumber(JSONPath path, BigDecimal number);

  void shredBoolean(JSONPath path, boolean value);

  void shredNull(JSONPath path);
}
