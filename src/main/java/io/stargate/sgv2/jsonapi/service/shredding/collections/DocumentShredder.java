package io.stargate.sgv2.jsonapi.service.shredding.collections;

import static io.stargate.sgv2.jsonapi.service.shredding.collections.JsonExtensionType.BINARY;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionIdType;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.types.ObjectId;

/**
 * Object that will shred an incoming JSON document into indexable entries we need to store in the
 * DB, to be able to support filtering queries.
 *
 * <p>Note that currently document id ({@code _id}) is auto-generated using UUID random method if
 * incoming JSON does not contain it (otherwise passed-in {@code _id} is used as-is).
 */
@ApplicationScoped
public class DocumentShredder {
  private static final NoArgGenerator UUID_V4_GENERATOR = Generators.randomBasedGenerator();
  private static final NoArgGenerator UUID_V6_GENERATOR = Generators.timeBasedReorderedGenerator();
  private static final NoArgGenerator UUID_V7_GENERATOR = Generators.timeBasedEpochGenerator();

  private final ObjectMapper objectMapper;

  private final DocumentLimitsConfig documentLimits;

  private final JsonProcessingMetricsReporter jsonProcessingMetricsReporter;

  @Inject
  public DocumentShredder(
      ObjectMapper objectMapper,
      DocumentLimitsConfig documentLimits,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    this.objectMapper = objectMapper;
    this.documentLimits = documentLimits;
    this.jsonProcessingMetricsReporter = jsonProcessingMetricsReporter;
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
    // TODO - why does this have "testCommand" for a command name ? Should it be a constant ?
    return shred(
        doc,
        txId,
        IndexingProjector.identityProjector(),
        "testCommand",
        CollectionSchemaObject.MISSING,
        null);
  }

  public WritableShreddedDocument shred(
      CommandContext<CollectionSchemaObject> ctx, JsonNode doc, UUID txId) {
    return shred(
        doc,
        txId,
        ctx.schemaObject().indexingProjector(),
        ctx.commandName(),
        ctx.schemaObject(),
        null);
  }

  /**
   * @param ctx Command context for processing, used for accessing Collection settings and indexing
   *     projector
   * @param doc Document to shred
   * @param txId (optional, nullable) transaction id used for avoiding race conditions
   * @param docIdToReturn (optional, nullable) Reference used for returning Document Id to caller,
   *     even if exception is thrown (set as soon as id is known)
   * @return Shredded document
   */
  public WritableShreddedDocument shred(
      CommandContext<CollectionSchemaObject> ctx,
      JsonNode doc,
      UUID txId,
      AtomicReference<DocumentId> docIdToReturn) {
    return shred(
        doc,
        txId,
        ctx.schemaObject().indexingProjector(),
        ctx.commandName(),
        ctx.schemaObject(),
        docIdToReturn);
  }

  public WritableShreddedDocument shred(
      JsonNode doc,
      UUID txId,
      IndexingProjector indexProjector,
      String commandName,
      CollectionSchemaObject collectionSettings,
      AtomicReference<DocumentId> docIdToReturn) {
    // Although we could otherwise allow non-Object documents, requirement
    // to have the _id (or at least place for it) means we cannot allow that.
    if (!doc.isObject()) {
      throw ErrorCodeV1.SHRED_BAD_DOCUMENT_TYPE.toApiException(
          "document to shred must be a JSON Object, instead got %s", doc.getNodeType());
    }

    final ObjectNode docWithId = normalizeDocumentId(collectionSettings, (ObjectNode) doc);
    final DocumentId docId = DocumentId.fromJson(docWithId.get(DocumentConstants.Fields.DOC_ID));
    final String docJson;

    if (docIdToReturn != null) {
      docIdToReturn.set(docId);
    }

    // Now that we have the traversable document, verify it does not violate
    // structural limits, before serializing.
    // (note: value validation has to wait until no-indexing projection is applied)
    new FullDocValidator(documentLimits).validate(docWithId);

    // Need to re-serialize document now that _id is normalized.
    // Also unifies escaping and gets rid of pretty-printing (if any) to save storage space.
    try {
      // Important! Must use configured ObjectMapper for serialization, NOT JsonNode.toString()
      // (to use configuration we specify wrt serialization)
      docJson = objectMapper.writeValueAsString(docWithId);
    } catch (JacksonException e) { // should never happen but signature exposes it
      throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
          e, "Failed to serialize document: %s", e.getMessage());
    }

    // And then we can validate the document size
    validateDocumentSize(documentLimits, docJson);

    // Create json bytes written metrics
    if (jsonProcessingMetricsReporter != null) {
      jsonProcessingMetricsReporter.reportJsonWriteBytesMetrics(commandName, docJson.length());
    }

    final WritableShreddedDocument.Builder b =
        WritableShreddedDocument.builder(docId, txId, docJson, docWithId);

    // Before value validation, indexing, may need to drop "non-indexed" properties. But if so,
    // need to ensure we do not modify original document, so let's create a copy (may need
    // to be returned as "after" Document)
    ObjectNode indexableDocument;

    if (indexProjector != null) {
      indexableDocument = docWithId.deepCopy();
      indexProjector.applyProjection(indexableDocument);
    } else {
      // optimized case: if nothing to drop ("no-index"), can just use original
      indexableDocument = docWithId;
    }

    // and now we can finally validate (String) value lengths
    new IndexableValueValidator(documentLimits).validate(indexableDocument);

    // And finally let's traverse the document to actually "shred" (build index properties)
    new ShreddingTraverser(b).traverse(indexableDocument);

    WritableShreddedDocument shreddedDoc = b.build();

    // Verify that "$lexical" field is not present if lexical indexing is disabled
    if (!collectionSettings.lexicalConfig().enabled() && shreddedDoc.queryLexicalValue() != null) {
      throw ErrorCodeV1.LEXICAL_NOT_ENABLED_FOR_COLLECTION.toApiException(
          "Document contains lexical content, but lexical indexing is not enabled for collection '%s'",
          collectionSettings.name().table());
    }

    return shreddedDoc;
  }

  /**
   * Method called to ensure that Document has Document Id (generating id if necessary), and that it
   * is the very first field in the document (reordering as needed). Note that a new document is
   * created and returned; input document is never modified.
   *
   * @param collectionSettings Collection settings to use for document id generation
   * @param doc Document to use as the base
   * @return Document that has _id as its first property
   */
  private ObjectNode normalizeDocumentId(
      CollectionSchemaObject collectionSettings, ObjectNode doc) {
    // First: see if we have Object Id present or not
    JsonNode idNode = doc.get(DocumentConstants.Fields.DOC_ID);

    // If not, generate one
    if (idNode == null) {
      idNode = generateDocumentId(collectionSettings);
    }
    // Either way we need to construct actual document with _id as the first property;
    // unfortunately there is no way to reorder properties in-place.
    final ObjectNode docWithIdAsFirstProperty = objectMapper.createObjectNode();
    docWithIdAsFirstProperty.set(DocumentConstants.Fields.DOC_ID, idNode);
    // Ok to add all properties, possibly including doc id since order won't change
    docWithIdAsFirstProperty.setAll(doc);
    return docWithIdAsFirstProperty;
  }

  private JsonNode generateDocumentId(CollectionSchemaObject collectionSettings) {
    CollectionIdType idType = collectionSettings.idConfig().idType();
    if (idType == null) {
      idType = CollectionIdType.UNDEFINED;
    }
    final JsonNodeFactory jnf = objectMapper.getNodeFactory();
    switch (idType) {
      case OBJECT_ID:
        return wrapExtensionType(jnf, JsonExtensionType.OBJECT_ID, new ObjectId());
      case UUID:
        return wrapExtensionType(jnf, JsonExtensionType.UUID, UUID_V4_GENERATOR.generate());
      case UUID_V6:
        return wrapExtensionType(jnf, JsonExtensionType.UUID, UUID_V6_GENERATOR.generate());
      case UUID_V7:
        return wrapExtensionType(jnf, JsonExtensionType.UUID, UUID_V7_GENERATOR.generate());
      case UNDEFINED:
    }
    // Default for "undefined"/"unspecified" is legacy unwrapped UUIDv4 (random)
    return jnf.textNode(UUID_V4_GENERATOR.generate().toString());
  }

  private static JsonNode wrapExtensionType(
      JsonNodeFactory jnf, JsonExtensionType etype, Object value) {
    return jnf.objectNode().put(etype.encodedName(), value.toString());
  }

  private void validateDocumentSize(DocumentLimitsConfig limits, String docJson) {
    // First: is the resulting document size (as serialized) too big?
    if (docJson.length() > limits.maxSize()) {
      throw ErrorCodeV1.SHRED_DOC_LIMIT_VIOLATION.toApiException(
          "document size (%d chars) exceeds maximum allowed (%d)",
          docJson.length(), limits.maxSize());
    }
  }

  /**
   * Validator applied to the full document, before removing non-indexable properties. Used to
   * ensure that the full document does not violate overall structural limits such as total length
   * or maximum nesting depth, or invalid field names. Most checks are done at a later point with
   * {@link IndexableValueValidator}.
   */
  static class FullDocValidator {
    final DocumentLimitsConfig limits;

    public FullDocValidator(DocumentLimitsConfig limits) {
      this.limits = limits;
    }

    public void validate(ObjectNode doc) {
      // Second: traverse to check for other constraints
      validateObjectValue(null, doc, 0, 0);
    }

    private void validateValue(
        String referringPropertyName, JsonNode value, int depth, int parentPathLength) {
      if (value.isObject()) {
        validateObjectValue(referringPropertyName, value, depth, parentPathLength);
      } else if (value.isArray()) {
        validateArrayValue(referringPropertyName, value, depth, parentPathLength);
      }
    }

    private void validateArrayValue(
        String referringPropertyName, JsonNode arrayValue, int depth, int parentPathLength) {
      ++depth;
      validateDocDepth(limits, depth);

      // Array value size limit only applied for indexable, none checked here
      for (JsonNode element : arrayValue) {
        validateValue(null, element, depth, parentPathLength);
      }
    }

    private void validateObjectValue(
        String referringPropertyName, JsonNode objectValue, int depth, int parentPathLength) {
      ++depth;
      validateDocDepth(limits, depth);

      // First, special case: Extension JSON types
      if (objectValue.size() == 1) {
        String key = objectValue.fieldNames().next();
        JsonExtensionType extType = JsonExtensionType.fromEncodedName(key);
        if (extType != null) {
          // These are only superficially validated here, more detailed validation
          // during actual shredding
          JsonNode value = objectValue.iterator().next();
          if (value.isTextual() || value.isIntegralNumber()) {
            return;
          }
          throw ErrorCodeV1.SHRED_BAD_EJSON_VALUE.toApiException(
              "type '%s' has invalid JSON value of type %s",
              extType.encodedName(), value.getNodeType());
        }
      }

      var it = objectValue.fields();
      while (it.hasNext()) {
        var entry = it.next();
        final String key = entry.getKey();

        // Doc id validation done elsewhere, skip here to avoid failure for
        // new Extension JSON types (Object-wrapped UUIDs, ObjectIds)
        if (depth == 1 && key.equals(DocumentConstants.Fields.DOC_ID)) {
          continue;
        }

        validateObjectKey(key, entry.getValue(), depth, parentPathLength);
        // Path through field consists of segments separated by periods:
        final int propPathLength = parentPathLength + 1 + key.length();
        validateValue(key, entry.getValue(), depth, propPathLength);
      }
    }

    private void validateObjectKey(String key, JsonNode value, int depth, int parentPathLength) {
      // NOTE: empty keys are allowed on v1.0.21 and later

      if (!NamingRules.FIELD.apply(key)) {
        // Special names are accepted in some cases:
        if ((depth == 1)
            && (key.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)
                || key.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)
                || key.equals(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD))) {
          ;
        } else {
          throw ErrorCodeV1.SHRED_DOC_KEY_NAME_VIOLATION.toApiException(
              "field name '%s' %s", key, key.isEmpty() ? "is empty" : "starts with '$'");
        }
      }
      int totalPathLength = parentPathLength + key.length();
      if (totalPathLength > limits.maxPropertyPathLength()) {
        throw ErrorCodeV1.SHRED_DOC_LIMIT_VIOLATION.toApiException(
            "field path length (%d) exceeds maximum allowed (%d) (path ends with '%s')",
            totalPathLength, limits.maxPropertyPathLength(), key);
      }
    }

    private void validateDocDepth(DocumentLimitsConfig limits, int depth) {
      if (depth > limits.maxDepth()) {
        throw ErrorCodeV1.SHRED_DOC_LIMIT_VIOLATION.toApiException(
            "document depth exceeds maximum allowed (%s)", limits.maxDepth());
      }
    }
  }

  /**
   * Secondary validator applied to the storable document after non-indexable properties (and
   * branches) have been pruned.
   */
  static class IndexableValueValidator {
    final DocumentLimitsConfig limits;

    final AtomicInteger totalProperties;

    public IndexableValueValidator(DocumentLimitsConfig limits) {
      this.limits = limits;
      totalProperties = new AtomicInteger(0);
    }

    public void validate(ObjectNode doc) {
      validateObjectValue(null, doc);
      if (totalProperties.get() > limits.maxDocumentProperties()) {
        throw ErrorCodeV1.SHRED_DOC_LIMIT_VIOLATION.toApiException(
            "total number of indexed properties (%d) in document exceeds maximum allowed (%d)",
            totalProperties.get(), limits.maxDocumentProperties());
      }
    }

    private void validateValue(String referringPropertyName, JsonNode value) {
      if (value.isObject()) {
        validateObjectValue(referringPropertyName, value);
      } else if (value.isArray()) {
        validateArrayValue(referringPropertyName, value);
      } else if (value.isTextual()) {
        validateStringValue(referringPropertyName, value.textValue());
      }
    }

    private void validateArrayValue(String referringPropertyName, JsonNode arrayValue) {
      if (arrayValue.size() > limits.maxArrayLength()) {
        // One special case: vector embeddings allow larger size
        if (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(referringPropertyName)) {
          if (arrayValue.size() > limits.maxVectorEmbeddingLength()) {
            throw ErrorCodeV1.SHRED_DOC_LIMIT_VIOLATION.toApiException(
                "number of elements Vector embedding (field '%s') has (%d) exceeds maximum allowed (%d)",
                referringPropertyName, arrayValue.size(), limits.maxVectorEmbeddingLength());
          }
        } else {
          throw ErrorCodeV1.SHRED_DOC_LIMIT_VIOLATION.toApiException(
              "number of elements an indexable Array (field '%s') has (%d) exceeds maximum allowed (%d)",
              referringPropertyName, arrayValue.size(), limits.maxArrayLength());
        }
      }

      for (JsonNode element : arrayValue) {
        validateValue(referringPropertyName, element);
      }
    }

    private void validateObjectValue(String referringPropertyName, JsonNode objectValue) {
      final int propCount = objectValue.size();
      if (propCount > limits.maxObjectProperties()) {
        throw ErrorCodeV1.SHRED_DOC_LIMIT_VIOLATION.toApiException(
            "number of properties an indexable Object (field '%s') has (%d) exceeds maximum allowed (%s)",
            referringPropertyName, objectValue.size(), limits.maxObjectProperties());
      }
      totalProperties.addAndGet(propCount);

      for (Map.Entry<String, JsonNode> entry : objectValue.properties()) {
        validateValue(entry.getKey(), entry.getValue());
      }
    }

    private void validateStringValue(String referringPropertyName, String value) {
      if (DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(referringPropertyName)
          || DocumentConstants.Fields.BINARY_VECTOR_TEXT_FIELD.equals(referringPropertyName)) {
        // `$vectorize`, `$binary` fields are not checked for length
        return;
      }
      if (DocumentConstants.Fields.LEXICAL_CONTENT_FIELD.equals(referringPropertyName)) {
        // '$lexical` field has different max length but not clear what it is: for now,
        // do not validate (add limit if we find out what SAI imposes)
        return;
      }

      OptionalInt encodedLength =
          JsonUtil.lengthInBytesIfAbove(value, limits.maxStringLengthInBytes());
      if (encodedLength.isPresent()) {
        throw ErrorCodeV1.SHRED_DOC_LIMIT_VIOLATION.toApiException(
            "indexed String value (field '%s') length (%d bytes) exceeds maximum allowed (%d bytes)",
            referringPropertyName, encodedLength.getAsInt(), limits.maxStringLengthInBytes());
      }
    }
  }

  /** Handler constructed for traversing JSON document and producing indexable properties. */
  static class ShreddingTraverser {
    private final DocumentShredderListener shredder;

    ShreddingTraverser(DocumentShredderListener shredder) {
      this.shredder = shredder;
    }

    /**
     * Main traversal method we need to produce callbacks to passed-in listener; used to separate
     * shredding logic from that of recursive-descent traversal.
     */
    public void traverse(JsonNode doc) {
      final JsonPath.Builder pathBuilder = JsonPath.rootBuilder();
      // NOTE: main level is handled a bit differently; no callbacks for Objects or Arrays,
      // only for the (rare) case of atomic values. Just traversal.

      if (doc.isObject()) {
        traverseObject((ObjectNode) doc, pathBuilder);
      } else if (doc.isArray()) {
        traverseArray((ArrayNode) doc, pathBuilder);
      } else {
        traverseValue(doc, pathBuilder);
      }
    }

    private void traverseObject(ObjectNode obj, JsonPath.Builder pathBuilder) {

      Iterator<Map.Entry<String, JsonNode>> it = obj.fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> entry = it.next();
        pathBuilder.property(DocumentPath.encodeSegment(entry.getKey()));
        traverseValue(entry.getValue(), pathBuilder);
      }
    }

    private void traverseArray(ArrayNode arr, JsonPath.Builder pathBuilder) {
      int ix = 0;
      for (JsonNode value : arr) {
        pathBuilder.index(ix++);
        traverseValue(value, pathBuilder);
      }
    }

    private void traverseValue(JsonNode value, JsonPath.Builder pathBuilder) {
      final JsonPath path = pathBuilder.build();
      final String pathAsString = path.toString();

      if (pathAsString.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
        traverseVector(path, value);
      } else if (pathAsString.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
        traverseVectorize(path, value);
      } else if (pathAsString.equals(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD)) {
        traverseLexical(path, value);
      } else {
        if (value.isObject()) {
          ObjectNode ob = (ObjectNode) value;
          if (shredder.shredObject(path, ob)) {
            traverseObject(ob, pathBuilder.nestedObjectBuilder());
          }
        } else if (value.isArray()) {
          ArrayNode arr = (ArrayNode) value;
          shredder.shredArray(path, arr);
          traverseArray(arr, pathBuilder.nestedArrayBuilder());
        } else if (value.isTextual()) {
          shredder.shredText(path, value.textValue());
        } else if (value.isNumber()) {
          shredder.shredNumber(path, value.decimalValue());
        } else if (value.isBoolean()) {
          shredder.shredBoolean(path, value.booleanValue());
        } else if (value.isNull()) {
          shredder.shredNull(path);
        } else {
          throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
              "Unsupported `JsonNodeType` in input document, `%s`", value.getNodeType());
        }
      }
    }

    private void traverseVector(JsonPath path, JsonNode value) {
      if (value.isNull()) {
        return;
      }

      // should be either array or object
      if (value.isArray()) {
        // e.g. "$vector": [0.25, 0.25]
        ArrayNode arr = (ArrayNode) value;
        if (arr.size() == 0) {
          throw ErrorCodeV1.SHRED_BAD_VECTOR_SIZE.toApiException();
        }
        shredder.shredVector(path, arr);
      } else if (value.isObject()) {
        // e.g. "$vector": {"$binary": "c3VyZS4="}
        ObjectNode obj = (ObjectNode) value;
        final Map.Entry<String, JsonNode> entry = obj.fields().next();
        JsonExtensionType keyType = JsonExtensionType.fromEncodedName(entry.getKey());
        if (keyType != BINARY) {
          throw ErrorCodeV1.SHRED_BAD_DOCUMENT_VECTOR_TYPE.toApiException(
              "The key for the %s object must be '%s'", path, BINARY.encodedName());
        }
        JsonNode binaryValue = entry.getValue();
        if (!binaryValue.isTextual()) {
          throw ErrorCodeV1.SHRED_BAD_BINARY_VECTOR_VALUE.toApiException(
              "Unsupported JSON value type in EJSON $binary wrapper (%s): only STRING allowed",
              binaryValue.getNodeType());
        }
        try {
          shredder.shredVector(path, binaryValue.binaryValue());
        } catch (IOException e) {
          throw ErrorCodeV1.SHRED_BAD_BINARY_VECTOR_VALUE.toApiException(
              "Invalid content in EJSON $binary wrapper: not valid Base64-encoded String, problem: %s"
                  .formatted(e.getMessage()));
        }
      } else {
        throw ErrorCodeV1.SHRED_BAD_DOCUMENT_VECTOR_TYPE.toApiException(
            value.getNodeType().toString());
      }
    }

    private void traverseVectorize(JsonPath path, JsonNode value) {
      if (!value.isNull()) {
        shredder.shredVectorize(path);
      }
    }

    private void traverseLexical(JsonPath path, JsonNode value) {
      if (value.isNull()) {
        return;
      }
      if (!value.isTextual()) {
        throw ErrorCodeV1.SHRED_BAD_DOCUMENT_LEXICAL_TYPE.toApiException(
            "the value for field '%s' must be a STRING, was: %s",
            path.toString(), value.getNodeType());
      }
      shredder.shredLexical(path, value.asText());
    }
  }
}
