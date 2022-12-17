package io.stargate.sgv3.docsapi.service.shredding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv3.docsapi.service.shredding.model.WritableShreddedDocument;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.enterprise.context.ApplicationScoped;
import javax.validation.constraints.NotNull;

/**
 * Shred an incoming JSON document into the data we need to store in the DB, and then de-shred.
 *
 * <p>This will be based on the ideas in the python lab, and extended to do things like make better
 * decisions about when to use a hash and when to use the actual value. i.e. a hash of "a" is a lot
 * longer than "a".
 */
@ApplicationScoped
public class Shredder {

  /**
   * Shreds a single JSON node into a {@link WritableShreddedDocument} representation.
   *
   * @param document {@link JsonNode} to shred.
   * @return WritableShreddedDocument
   */
  public WritableShreddedDocument shred(@NotNull JsonNode document) {
    return shred(document, Optional.empty());
  }

  public WritableShreddedDocument shred(@NotNull JsonNode doc, @NotNull Optional<UUID> txId) {
    // 13-Dec-2022, tatu: Although we could otherwise allow non-Object documents, requirement
    //    to have the _id (or at least place for it) means we cannot allow that.
    if (!doc.isObject()) {
      throw failure("Document to shred must be a JSON Object: instead got %s", doc.getNodeType());
    }
    ObjectNode docOb = (ObjectNode) doc;

    // We will extract id if there is one; stored separately, removed so we won't process
    // it, add path or such (since it has specific meaning separate from general properties)
    JsonNode idNode = docOb.remove("_id");
    final String id;

    // Cannot require existence as id often auto-generated when inserting: caller has to verify
    // if existence required
    if (idNode == null) {
      id = null;
    } else {
      if (!idNode.isTextual()) {
        throw failure("Bad type for '_id' property (%s)", idNode.getNodeType());
      }
      id = idNode.asText();
    }

    final WritableShreddedDocument.Builder b =
        WritableShreddedDocument.builder(new DocValueHasher(), id, txId);
    traverse(doc, b, JSONPath.rootBuilder());
    return b.build();
  }

  private RuntimeException failure(String format, Object... args) {
    // TODO: use proper exception type once we have it
    throw new RuntimeException(String.format(format, args));
  }

  /**
   * Main traversal method we need to produce callbacks to passed-in listener; used to separate
   * shredding logic from that of recursive-descent traversal.
   */
  private void traverse(JsonNode doc, ShredListener callback, JSONPath.Builder pathBuilder) {
    // NOTE: main level is handled bit differently; no callbacks for Objects or Arrays,
    // only for the (rare) case of atomic values. Just traversal.

    if (doc.isObject()) {
      traverseObject((ObjectNode) doc, callback, pathBuilder);
    } else if (doc.isArray()) {
      traverseArray((ArrayNode) doc, callback, pathBuilder);
    } else {
      traverseValue(doc, callback, pathBuilder);
    }
  }

  private void traverseObject(
      ObjectNode obj, ShredListener callback, JSONPath.Builder pathBuilder) {

    Iterator<Map.Entry<String, JsonNode>> it = obj.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      pathBuilder.property(entry.getKey());
      traverseValue(entry.getValue(), callback, pathBuilder);
    }
  }

  private void traverseArray(ArrayNode arr, ShredListener callback, JSONPath.Builder pathBuilder) {
    int ix = 0;
    for (JsonNode value : arr) {
      pathBuilder.index(ix++);
      traverseValue(value, callback, pathBuilder);
    }
  }

  private void traverseValue(JsonNode value, ShredListener callback, JSONPath.Builder pathBuilder) {
    if (value.isObject()) {
      ObjectNode ob = (ObjectNode) value;
      callback.shredObject(pathBuilder, ob);
      traverseObject(ob, callback, pathBuilder.nestedValueBuilder());
    } else if (value.isArray()) {
      ArrayNode arr = (ArrayNode) value;
      callback.shredArray(pathBuilder, arr);
      traverseArray(arr, callback, pathBuilder.nestedValueBuilder());
    } else if (value.isTextual()) {
      callback.shredText(pathBuilder.build(), value.textValue());
    } else if (value.isNumber()) {
      callback.shredNumber(pathBuilder.build(), value.decimalValue());
    } else if (value.isBoolean()) {
      callback.shredBoolean(pathBuilder.build(), value.booleanValue());
    } else if (value.isNull()) {
      callback.shredNull(pathBuilder.build());
    } else {
      throw failure("Unrecognized `JsonNode` type: %s", value.getNodeType());
    }
  }
}
