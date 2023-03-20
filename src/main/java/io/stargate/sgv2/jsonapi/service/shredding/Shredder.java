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
    validateDocument(docWithId, documentLimits);

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

  private void validateDocument(ObjectNode doc, DocumentLimitsConfig limits) {
    ; // TO IMPLEMENT
  }
}
