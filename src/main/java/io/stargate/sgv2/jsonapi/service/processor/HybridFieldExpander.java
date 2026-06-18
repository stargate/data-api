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
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.metrics.CommandFeature;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.Iterator;
import java.util.Map;

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
    // Only support expansion for Collections
    if (context.isCollectionContext()) {
      // and just for Insert commands, in particular
      switch (command) {
        case InsertOneCommand cmd -> expandHybridField(context, 0, 1, cmd.document());
        case InsertManyCommand cmd -> {
          var docs = cmd.documents();
          if (docs != null) {
            for (int i = 0, len = docs.size(); i < len; ++i) {
              expandHybridField(context, i, len, docs.get(i));
            }
          }
        }
        default -> {}
      }
    }
  }

  // protected for testing purposes
  protected static void expandHybridField(
      CommandContext context, int docIndex, int docCount, JsonNode docNode) {
    final JsonNode hybridField;

    if (docNode instanceof ObjectNode doc) {
      // Check for $hybrid field
      if ((hybridField = doc.remove(DocumentConstants.Fields.HYBRID_FIELD)) != null) {
        context.commandFeatures().addFeature(CommandFeature.HYBRID);
        switch (hybridField) {
            // this is {"$hybrid" : null}
          case NullNode ignored -> addLexicalAndVectorize(doc, hybridField, hybridField);
          case TextNode ignored -> addLexicalAndVectorize(doc, hybridField, hybridField);
          case ObjectNode ob -> addFromObject(context, doc, ob, docIndex, docCount);
          default ->
              throw RequestException.Code.HYBRID_FIELD_UNSUPPORTED_VALUE_TYPE.get(
                  "errorMessage",
                  "expected String, Object or `null` but received %s (Document %d of %d)"
                      .formatted(JsonUtil.nodeTypeAsString(hybridField), docIndex + 1, docCount));
        }
      } else {
        // No $hybrid field, check other fields and add feature usage to CommandContext
        if (doc.has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)
            && !doc.get(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD).isNull()) {
          context.commandFeatures().addFeature(CommandFeature.VECTOR);
        }
        // `$vectorize` and `$vector` can't be used together - the check will be done later (in
        // DataVectorizer)
        if (doc.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)
            && !doc.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD).isNull()) {
          context.commandFeatures().addFeature(CommandFeature.VECTORIZE);
        }
        if (doc.has(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD)
            && !doc.get(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD).isNull()) {
          context.commandFeatures().addFeature(CommandFeature.LEXICAL);
        }
      }
    }
  }

  private static void addFromObject(
      CommandContext context, ObjectNode doc, ObjectNode hybrid, int docIndex, int docCount) {
    JsonNode lexical = hybrid.remove(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD);
    JsonNode vectorize = hybrid.remove(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);

    // Any unknown fields? Report
    if (!hybrid.isEmpty()) {
      Iterator<CharSequence> it = (Iterator<CharSequence>) (Iterator<?>) hybrid.fieldNames();
      throw RequestException.Code.HYBRID_FIELD_UNKNOWN_SUBFIELDS.get(
          Map.of(
              "errorMessage",
              "expected '$lexical' and/or '$vectorize' but encountered: '%s' (Document %d of %d)"
                  .formatted(String.join("', '", () -> it), docIndex + 1, docCount)));
    }

    // Also: must validate $lexical, $vectorize value types if not null (missing)
    // (note: ok to miss one or both)
    lexical =
        validateSubFieldType(
            lexical, DocumentConstants.Fields.LEXICAL_CONTENT_FIELD, docIndex, docCount);
    if (!lexical.isNull()) {
      context.commandFeatures().addFeature(CommandFeature.LEXICAL);
    }
    vectorize =
        validateSubFieldType(
            vectorize, DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD, docIndex, docCount);
    if (!vectorize.isNull()) {
      context.commandFeatures().addFeature(CommandFeature.VECTORIZE);
    }

    addLexicalAndVectorize(doc, lexical, vectorize);
  }

  private static JsonNode validateSubFieldType(
      JsonNode value, String subfield, int docIndex, int docCount) {
    return switch (value) {
      case NullNode ignored -> value;
      case TextNode ignored -> value;
      case null -> NullNode.getInstance();
      default ->
          throw RequestException.Code.HYBRID_FIELD_UNSUPPORTED_SUBFIELD_VALUE_TYPE.get(
              Map.of(
                  "errorMessage",
                  "expected String or `null` for '%s' but received %s (Document %d of %d)"
                      .formatted(
                          subfield, JsonUtil.nodeTypeAsString(value), docIndex + 1, docCount)));
    };
  }

  private static void addLexicalAndVectorize(ObjectNode doc, JsonNode lexical, JsonNode vectorize) {
    // Important: verify we had no conflict with existing $lexical or $vector or $vectorize fields
    // (that is, values from $hybrid would not overwrite existing values)
    var oldLexical = doc.replace(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD, lexical);
    var oldVector = doc.get(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
    var oldVectorize = doc.replace(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD, vectorize);

    if ((oldLexical != null) || (oldVector != null) || (oldVectorize != null)) {
      throw RequestException.Code.HYBRID_FIELD_CONFLICT.get();
    }
  }
}
