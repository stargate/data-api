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
        case InsertOneCommand cmd -> expandHybridField(context, command, 0, cmd.document());
        case InsertManyCommand cmd -> {
          var docs = cmd.documents();
          if (docs != null) {
            for (int i = 0; i < docs.size(); i++) {
              expandHybridField(context, command, i, docs.get(i));
            }
          }
        }
        default -> {}
      }
    }
  }

  private static void expandHybridField(
      CommandContext context, Command command, int index, JsonNode docNode) {
    final JsonNode hybridField;

    if (!(docNode instanceof ObjectNode doc)
        || (hybridField = doc.remove(DocumentConstants.Fields.HYBRID_FIELD)) == null) {
      return;
    }

    switch (hybridField) {
      case NullNode ignored -> addLexicalAndVectorize(doc, hybridField, hybridField);
      case TextNode ignored -> addLexicalAndVectorize(doc, hybridField, hybridField);
        // case ObjectNode ob ->
      default -> {
        throw ErrorCodeV1.HYBRID_FIELD_VALUE_TYPE_UNSUPPORTED.toApiException(
            "expected String, null or Object but received %s",
            JsonUtil.nodeTypeAsString(hybridField));
      }
    }
  }

  private static void addLexicalAndVectorize(ObjectNode doc, JsonNode lexical, JsonNode vectorize) {
    doc.put(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD, lexical);
    doc.put(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD, vectorize);
  }
}
