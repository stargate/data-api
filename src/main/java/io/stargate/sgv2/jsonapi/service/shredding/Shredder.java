package io.stargate.sgv2.jsonapi.service.shredding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.stargate.sgv2.jsonapi.api.v1.metrics.DefaultJsonSerializationMetrics;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessMetrics;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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

  private final MeterRegistry meterRegistry;

  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public Shredder(
      ObjectMapper objectMapper,
      DocumentLimitsConfig documentLimits,
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.objectMapper = objectMapper;
    this.documentLimits = documentLimits;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  /**
   * Wrapper function for shred function to add serialization metrics
   *
   * @param doc {@link JsonNode} to shred.
   * @param commandName command name
   * @param serializationMetrics add metrics for serialization
   * @return WritableShreddedDocument
   */
  public WritableShreddedDocument shredWithMetrics(
      JsonNode doc,
      String commandName,
      JsonProcessMetrics serializationMetrics) {
    return shredWithMetrics(doc, null, commandName, serializationMetrics);
  }

  public WritableShreddedDocument shredWithMetrics(
      JsonNode doc,
      UUID txId,
      String commandName,
      JsonProcessMetrics serializationMetrics) {

    // Use the provided metrics handler or the default one if none is provided
    JsonProcessMetrics effectiveMetrics =
        serializationMetrics != null
            ? serializationMetrics
            : new DefaultJsonSerializationMetrics(meterRegistry, jsonApiMetricsConfig);

    if (effectiveMetrics instanceof DefaultJsonSerializationMetrics) {
      // Start the timer
      Timer.Sample sample = Timer.start(meterRegistry);
      // Call the original shred method
      WritableShreddedDocument shreddedDocument = shred(doc, txId);
      // Produce metrics after shredding is done
      effectiveMetrics.addMetrics(sample, commandName);
      return shreddedDocument;
    }

    return shred(doc);
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
    // Although we could otherwise allow non-Object documents, requirement
    // to have the _id (or at least place for it) means we cannot allow that.
    if (!doc.isObject()) {
      throw new JsonApiException(
          ErrorCode.SHRED_BAD_DOCUMENT_TYPE,
          String.format(
              "%s: Document to shred must be a JSON Object, instead got %s",
              ErrorCode.SHRED_BAD_DOCUMENT_TYPE.getMessage(), doc.getNodeType()));
    }

    final ObjectNode docWithId = normalizeDocumentId((ObjectNode) doc);
    final DocumentId docId = DocumentId.fromJson(docWithId.get(DocumentConstants.Fields.DOC_ID));
    final String docJson;

    // Need to re-serialize document now that _id is normalized.
    // Also gets rid of pretty-printing (if any) and unifies escaping.
    try {
      // Important! Must use configured ObjectMapper for serialization, NOT JsonNode.toString()
      docJson = objectMapper.writeValueAsString(docWithId);
    } catch (IOException e) { // never happens but signature exposes it
      throw new RuntimeException(e);
    }
    // Now that we have both the traversable document and serialization, verify
    // it does not violate document limits:
    validateDocument(documentLimits, docWithId, docJson);

    final WritableShreddedDocument.Builder b =
        WritableShreddedDocument.builder(docId, txId, docJson, docWithId);

    // And finally let's traverse the document to actually "shred" (build index fields)
    traverse(docWithId, b, JsonPath.rootBuilder());
    return b.build();
  }

  /**
   * Method called to ensure that Document has Document Id (generating id if necessary), and that it
   * is the very first field in the document (reordering as needed). Note that a new document is
   * created and returned; input document is never modified.
   *
   * @param doc Document to use as the base
   * @return Document that has _id as its first field
   */
  private ObjectNode normalizeDocumentId(ObjectNode doc) {
    // First: see if we have Object Id present or not
    JsonNode idNode = doc.get(DocumentConstants.Fields.DOC_ID);

    // If not, generateone
    if (idNode == null) {
      idNode = generateDocumentId();
    }
    // Either way we need to construct actual document with _id as the first field;
    // unfortunately there is no way to reorder fields in-place.
    final ObjectNode docWithIdAsFirstField = objectMapper.createObjectNode();
    docWithIdAsFirstField.set(DocumentConstants.Fields.DOC_ID, idNode);
    // Ok to add all fields, possibly including doc id since order won't change
    docWithIdAsFirstField.setAll(doc);
    return docWithIdAsFirstField;
  }

  private JsonNode generateDocumentId() {
    // Currently we generate UUID-as-String; alternatively could use and create
    // ObjectId-compatible values for better interoperability
    return objectMapper.getNodeFactory().textNode(UUID.randomUUID().toString());
  }

  /**
   * Main traversal method we need to produce callbacks to passed-in listener; used to separate
   * shredding logic from that of recursive-descent traversal.
   */
  private void traverse(JsonNode doc, ShredListener callback, JsonPath.Builder pathBuilder) {
    // NOTE: main level is handled a bit differently; no callbacks for Objects or Arrays,
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
    if (path.toString().equals(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
      if (value.isNull()) {
        return;
      }
      if (!value.isArray()) {
        throw new JsonApiException(
            ErrorCode.SHRED_BAD_DOCUMENT_VECTOR_TYPE,
            String.format(
                "%s: %s",
                ErrorCode.SHRED_BAD_DOCUMENT_VECTOR_TYPE.getMessage(), value.getNodeType()));
      }
      ArrayNode arr = (ArrayNode) value;
      if (arr.size() == 0) {
        throw new JsonApiException(
            ErrorCode.SHRED_BAD_VECTOR_SIZE, ErrorCode.SHRED_BAD_VECTOR_SIZE.getMessage());
      }
      callback.shredVector(path, arr);
    } else if (path.toString().equals(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
      // Do nothing, vectorize field will just sit in doc json
    } else {
      if (value.isObject()) {
        ObjectNode ob = (ObjectNode) value;
        if (callback.shredObject(path, ob)) {
          traverseObject(ob, callback, pathBuilder.nestedObjectBuilder());
        }
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
                "%s: %s",
                ErrorCode.SHRED_UNRECOGNIZED_NODE_TYPE.getMessage(), value.getNodeType()));
      }
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
    AtomicInteger totalProperties = new AtomicInteger(0);
    validateObjectValue(limits, null, doc, 0, totalProperties);
    if (totalProperties.get() > limits.maxDocumentProperties()) {
      throw new JsonApiException(
          ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
          String.format(
              "%s: total number of properties (%d) in document exceeds maximum allowed (%d)",
              ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(),
              totalProperties.get(),
              limits.maxDocumentProperties()));
    }
  }

  private void validateDocValue(
      DocumentLimitsConfig limits,
      String referringPropertyName,
      JsonNode value,
      int depth,
      AtomicInteger totalProperties) {
    if (value.isObject()) {
      validateObjectValue(limits, referringPropertyName, value, depth, totalProperties);
    } else if (value.isArray()) {
      validateArrayValue(limits, referringPropertyName, value, depth, totalProperties);
    } else if (value.isTextual()) {
      validateStringValue(limits, value);
    }
  }

  private void validateArrayValue(
      DocumentLimitsConfig limits,
      String referringPropertyName,
      JsonNode arrayValue,
      int depth,
      AtomicInteger totalProperties) {
    ++depth;
    validateDocDepth(limits, depth);

    if (arrayValue.size() > limits.maxArrayLength()) {
      // One special case: vector embeddings allow larger size
      if (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(referringPropertyName)) {
        if (arrayValue.size() > limits.maxVectorEmbeddingLength()) {
          throw new JsonApiException(
              ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
              String.format(
                  "%s: number of elements Vector embedding ('%s') has (%d) exceeds maximum allowed (%s)",
                  ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(),
                  referringPropertyName,
                  arrayValue.size(),
                  limits.maxVectorEmbeddingLength()));
        }
      } else {
        throw new JsonApiException(
            ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
            String.format(
                "%s: number of elements an Array has (%d) exceeds maximum allowed (%s)",
                ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(),
                arrayValue.size(),
                limits.maxArrayLength()));
      }
    }

    for (JsonNode element : arrayValue) {
      validateDocValue(limits, null, element, depth, totalProperties);
    }
  }

  private void validateObjectValue(
      DocumentLimitsConfig limits,
      String referringPropertyName,
      JsonNode objectValue,
      int depth,
      AtomicInteger totalProperties) {
    ++depth;
    validateDocDepth(limits, depth);

    final int propCount = objectValue.size();
    if (propCount > limits.maxObjectProperties()) {
      throw new JsonApiException(
          ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
          String.format(
              "%s: number of properties an Object has (%d) exceeds maximum allowed (%s)",
              ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(),
              objectValue.size(),
              limits.maxObjectProperties()));
    }
    totalProperties.addAndGet(propCount);

    var it = objectValue.fields();
    while (it.hasNext()) {
      var entry = it.next();
      validateObjectKey(limits, entry.getKey(), entry.getValue(), depth);
      validateDocValue(limits, entry.getKey(), entry.getValue(), depth, totalProperties);
    }
  }

  private void validateObjectKey(
      DocumentLimitsConfig limits, String key, JsonNode value, int depth) {
    if (key.length() > documentLimits.maxPropertyNameLength()) {
      throw new JsonApiException(
          ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
          String.format(
              "%s: Property name length (%d) exceeds maximum allowed (%s) (name '%s')",
              ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(),
              key.length(),
              limits.maxPropertyNameLength(),
              key));
    }
    if (key.length() == 0) {
      // NOTE: validity failure, not size limit
      throw new JsonApiException(
          ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION,
          String.format(
              "%s: empty names not allowed", ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION.getMessage()));
    }
    if (!DocumentConstants.Fields.VALID_NAME_PATTERN.matcher(key).matches()) {
      // Special names are accepted in some cases: for now only one such case for
      // Date values -- if more needed, will refactor. But for now inline:
      if (JsonUtil.EJSON_VALUE_KEY_DATE.equals(key) && value.isValueNode()) {
        ; // Fine, looks like legit Date value
      } else if (key.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD) && depth == 1) {
        ; // Fine, looks like legit vector field
      } else if (key.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD) && depth == 1) {
        ; // Fine, looks like legit vectorize field
      } else {
        throw new JsonApiException(
            ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION,
            String.format(
                "%s: Property name ('%s') contains character(s) not allowed",
                ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION.getMessage(), key));
      }
    }
  }

  private void validateStringValue(DocumentLimitsConfig limits, JsonNode stringValue) {
    final String value = stringValue.textValue();
    OptionalInt encodedLength =
        JsonUtil.lengthInBytesIfAbove(value, limits.maxStringLengthInBytes());
    if (encodedLength.isPresent()) {
      throw new JsonApiException(
          ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
          String.format(
              "%s: String value length (%d bytes) exceeds maximum allowed (%d bytes)",
              ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage(),
              encodedLength.getAsInt(),
              limits.maxStringLengthInBytes()));
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
