package io.stargate.sgv2.jsonapi.service.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.Iterator;

/**
 * Helper class that encapsulates the logic for expanding "$hybrid" fields in Collection Documents
 * to insert.
 *
 * <p>Note that this expansion is done before checking whether either lexical or vectorize fields
 * are actually supported -- this is verified at later point. Note, too, that input document(s) are
 * modified in place, due to design of "$vectorize" support for Collections.
 */
public class HybridFieldExpander {
  public static void expandHybridField(CommandContext context, Command command) {
    // Only support for Collections
    if (context.isCollectionContext()) {
      // and just for Insert commands, in particular
      switch (command) {
        case InsertOneCommand cmd -> expandHybridField(0, 1, cmd.document());
        case InsertManyCommand cmd -> {
          var docs = cmd.documents();
          if (docs != null) {
            for (int i = 0, len = docs.size(); i < len; ++i) {
              expandHybridField(i, len, docs.get(i));
            }
          }
        }
        default -> {}
      }
    }
  }

  // protected for testing purposes
  protected static void expandHybridField(int docIndex, int docCount, JsonNode docNode) {
    final JsonNode hybridField;

    if ((docNode instanceof ObjectNode doc)
        && (hybridField = doc.remove(DocumentConstants.Fields.HYBRID_FIELD)) != null) {
      switch (hybridField) {
        case NullNode ignored -> addLexicalAndVectorize(doc, hybridField, hybridField);
        case TextNode ignored -> addLexicalAndVectorize(doc, hybridField, hybridField);
        case ObjectNode ob -> addFromObject(doc, ob, docIndex, docCount);
        default ->
            throw ErrorCodeV1.HYBRID_FIELD_UNSUPPORTED_VALUE_TYPE.toApiException(
                "expected String, Object or `null` but received %s (Document %d of %d)",
                JsonUtil.nodeTypeAsString(hybridField), docIndex + 1, docCount);
      }
    }
  }

  private static void addFromObject(ObjectNode doc, ObjectNode hybrid, int docIndex, int docCount) {
    JsonNode lexical = hybrid.remove(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD);
    JsonNode vectorize = hybrid.remove(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);

    // Any unknown fields? Report
    if (!hybrid.isEmpty()) {
      Iterator<CharSequence> it = (Iterator<CharSequence>) (Iterator<?>) hybrid.fieldNames();
      throw ErrorCodeV1.HYBRID_FIELD_UNKNOWN_SUBFIELDS.toApiException(
          "expected '$lexical' and/or '$vectorize' but encountered: '%s' (Document %d of %d)",
          String.join("', '", () -> it), docIndex + 1, docCount);
    }

    // Also: must validate $lexical, $vectorize value types if not null (missing)
    // (note: ok to miss one or both)
    lexical =
        validateSubFieldType(
            lexical, DocumentConstants.Fields.LEXICAL_CONTENT_FIELD, docIndex, docCount);
    vectorize =
        validateSubFieldType(
            vectorize, DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD, docIndex, docCount);

    addLexicalAndVectorize(doc, lexical, vectorize);
  }

  private static JsonNode validateSubFieldType(
      JsonNode value, String subfield, int docIndex, int docCount) {
    return switch (value) {
      case NullNode ignored -> value;
      case TextNode ignored -> value;
      case null -> NullNode.getInstance();
      default ->
          throw ErrorCodeV1.HYBRID_FIELD_UNSUPPORTED_SUBFIELD_VALUE_TYPE.toApiException(
              "expected String or `null` for '%s' but received %s (Document %d of %d)",
              subfield, JsonUtil.nodeTypeAsString(value), docIndex + 1, docCount);
    };
  }

  private static void addLexicalAndVectorize(ObjectNode doc, JsonNode lexical, JsonNode vectorize) {
    doc.put(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD, lexical);
    doc.put(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD, vectorize);
  }
}
