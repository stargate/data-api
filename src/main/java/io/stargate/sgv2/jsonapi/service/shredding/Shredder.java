package io.stargate.sgv2.jsonapi.service.shredding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Shred an incoming JSON document into the data we need to store in the DB, and then de-shred.
 *
 * <p>Implementation is based on the ideas from the earlier prototype, and extended to do things
 * like make better decisions about when to use a hash and when to use the actual value. i.e. a hash
 * of "a" is a lot longer than "a".
 *
 * <p>Note that currently document id ({@code _id}) is auto-generated using UUID random method if
 * incoming JSON does not contain it (otherwise passed-in {@code _id} is used as-is).
 */
@ApplicationScoped
public class Shredder {
  private final ObjectMapper objectMapper;

  private final DocumentLimitsConfig documentLimits;

  @Inject
  public Shredder(ObjectMapper objectMapper, DocumentLimitsConfig documentLimits) {
    this.objectMapper = objectMapper;
    this.documentLimits = documentLimits;
  }

  /**
   * Shreds a single JSON node into a {@link WritableShreddedDocument} representation.
   *
   * @param document {@link JsonNode} to shred.
   * @return WritableShreddedDocument
   */
  public WritableShreddedDocument shred(JsonNode document) {
    return shred(document, null);
  }

  public WritableShreddedDocument shred(JsonNode doc, UUID txId) {
    // 13-Dec-2022, tatu: Although we could otherwise allow non-Object documents, requirement
    //    to have the _id (or at least place for it) means we cannot allow that.
    if (!doc.isObject()) {
      throw new JsonApiException(
          ErrorCode.SHRED_BAD_DOCUMENT_TYPE,
          String.format(
              "%s: Document to shred must be a JSON Object, instead got %s",
              ErrorCode.SHRED_BAD_DOCUMENT_TYPE.getMessage(), doc.getNodeType()));
    }

    // We will extract id if there is one; stored separately, but also included in JSON document
    // before storing in persistence. Need to make copy to avoid modifying input doc
    ObjectNode docWithoutId = ((ObjectNode) doc).objectNode().setAll((ObjectNode) doc);
    JsonNode idNode = docWithoutId.remove(DocumentConstants.Fields.DOC_ID);

    // We will use `_id`, if passed (but must be JSON String or Number); if not passed,
    // need to generate
    DocumentId docId = (idNode == null) ? generateDocumentId() : DocumentId.fromJson(idNode);

    // We will re-serialize document; gets rid of pretty-printing (if any);
    // unifies escaping.
    // NOTE! Since we removed "_id" if it existed (and generated if it didn't),
    // need to add back. Moreover we want it as the FIRST field anyway so need
    // to reconstruct document.
    ObjectNode docWithId = docWithoutId.objectNode(); // simple constructor, not linked
    docWithId.set(DocumentConstants.Fields.DOC_ID, docId.asJson(objectMapper));
    docWithId.setAll(docWithoutId);

    // Important! Must use configured ObjectMapper for serialization, NOT JsonNode.toString()
    final String docJson;

    try {
      docJson = objectMapper.writeValueAsString(docWithId);
    } catch (IOException e) { // never happens but signature exposes it
      throw new RuntimeException(e);
    }
    // Now that we have both the traversable document and serialization, verify
    // it does not violate document limits:
    validateDocument(documentLimits, docWithId, docJson);

    final WritableShreddedDocument.Builder b =
        WritableShreddedDocument.builder(new DocValueHasher(), docId, txId, docJson);

    // And now let's traverse the document, _including DocumentId so it will also
    // be indexed along with other fields.
    traverse(docWithId, b, JsonPath.rootBuilder());
    return b.build();
  }

  private DocumentId generateDocumentId() {
    return DocumentId.fromUUID(UUID.randomUUID());
  }

  /**
   * Main traversal method we need to produce callbacks to passed-in listener; used to separate
   * shredding logic from that of recursive-descent traversal.
   */
  private void traverse(JsonNode doc, ShredListener callback, JsonPath.Builder pathBuilder) {
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
      ObjectNode obj, ShredListener callback, JsonPath.Builder pathBuilder) {

    Iterator<Map.Entry<String, JsonNode>> it = obj.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      pathBuilder.property(entry.getKey());
      traverseValue(entry.getValue(), callback, pathBuilder);
    }
  }

  private void traverseArray(ArrayNode arr, ShredListener callback, JsonPath.Builder pathBuilder) {
    int ix = 0;
    for (JsonNode value : arr) {
      pathBuilder.index(ix++);
      traverseValue(value, callback, pathBuilder);
    }
  }

  private void traverseValue(JsonNode value, ShredListener callback, JsonPath.Builder pathBuilder) {
    final JsonPath path = pathBuilder.build();
    if (value.isObject()) {
      ObjectNode ob = (ObjectNode) value;
      callback.shredObject(path, ob);
      traverseObject(ob, callback, pathBuilder.nestedObjectBuilder());
    } else if (value.isArray()) {
      ArrayNode arr = (ArrayNode) value;
      callback.shredArray(path, arr);
      traverseArray(arr, callback, pathBuilder.nestedArrayBuilder());
    } else if (value.isTextual()) {
      callback.shredText(path, value.textValue());
    } else if (value.isNumber()) {
      callback.shredNumber(path, value.decimalValue());
    } else if (value.isBoolean()) {
      callback.shredBoolean(path, value.booleanValue());
    } else if (value.isNull()) {
      callback.shredNull(path);
    } else {
      throw new JsonApiException(
          ErrorCode.SHRED_UNRECOGNIZED_NODE_TYPE,
          String.format(
              "%s: %s", ErrorCode.SHRED_UNRECOGNIZED_NODE_TYPE.getMessage(), value.getNodeType()));
    }
  }

  private void validateDocument(DocumentLimitsConfig limits, ObjectNode doc, String docJson) {
    // First: is the resulting document size (as serialized) too big?
    if (docJson.length() > limits.maxSize()) {
      throw new JsonApiException(
          ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
          String.format(
              "%s: document size (%d chars) exceeds maximum allowed (%d)",
              ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(),
              docJson.length(),
              limits.maxSize()));
    }

    // Second: traverse to check for other constraints
    validateObjectValue(limits, doc, 0);
  }

  private void validateDocValue(DocumentLimitsConfig limits, JsonNode value, int depth) {
    if (value.isObject()) {
      validateObjectValue(limits, value, depth);
    } else if (value.isArray()) {
      validateArrayValue(limits, value, depth);
    } else if (value.isTextual()) {
      validateStringValue(limits, value);
    }
  }

  private void validateArrayValue(DocumentLimitsConfig limits, JsonNode arrayValue, int depth) {
    ++depth;
    validateDocDepth(limits, depth);

    if (arrayValue.size() > limits.maxArrayLength()) {
      throw new JsonApiException(
          ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
          String.format(
              "%s: number of elements an Array has (%d) exceeds maximum allowed (%s)",
              ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(),
              arrayValue.size(),
              limits.maxArrayLength()));
    }

    for (JsonNode element : arrayValue) {
      validateDocValue(limits, element, depth);
    }
  }

  private void validateObjectValue(DocumentLimitsConfig limits, JsonNode objectValue, int depth) {
    ++depth;
    validateDocDepth(limits, depth);

    if (objectValue.size() > limits.maxObjectProperties()) {
      throw new JsonApiException(
          ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
          String.format(
              "%s: number of properties an Object has (%d) exceeds maximum allowed (%s)",
              ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(),
              objectValue.size(),
              limits.maxObjectProperties()));
    }

    var it = objectValue.fields();
    while (it.hasNext()) {
      var entry = it.next();
      final String key = entry.getKey();
      if (key.length() > documentLimits.maxNameLength()) {
        throw new JsonApiException(
            ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
            String.format(
                "%s: Property name length (%d) exceeds maximum allowed (%s)",
                ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(),
                key.length(),
                limits.maxNameLength()));
      }
      validateDocValue(limits, entry.getValue(), depth);
    }
  }

  private void validateStringValue(DocumentLimitsConfig limits, JsonNode stringValue) {
    final String value = stringValue.textValue();
    if (value.length() > limits.maxStringLength()) {
      throw new JsonApiException(
          ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
          String.format(
              "%s: String value length (%d) exceeds maximum allowed (%s)",
              ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(),
              value.length(),
              limits.maxStringLength()));
    }
  }

  private void validateDocDepth(DocumentLimitsConfig limits, int depth) {
    if (depth > limits.maxDepth()) {
      throw new JsonApiException(
          ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
          String.format(
              "%s: document depth exceeds maximum allowed (%s)",
              ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(), limits.maxDepth()));
    }
  }
}
