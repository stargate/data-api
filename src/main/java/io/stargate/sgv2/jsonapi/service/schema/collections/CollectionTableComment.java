package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;

/**
 * Helpers for the JSON stored as the CQL {@code comment} table option of a Collection's backing
 * table. Centralizes "where the comment lives" and "what a V1 comment looks like" so callers (e.g.
 * createCollection / alterCollection / settings parsing) do not each re-derive it.
 */
public final class CollectionTableComment {

  private static final CqlIdentifier COMMENT_OPTION = CqlIdentifier.fromInternal("comment");

  private CollectionTableComment() {}

  /** The raw comment string stored on the table, or {@code null} if there is none. */
  public static String rawComment(TableMetadata table) {
    Object comment = table.getOptions().get(COMMENT_OPTION);
    return comment == null ? null : comment.toString();
  }

  /**
   * Whether the table carries a V1-shaped comment, i.e. one with a {@code collection.options} JSON
   * object. Legacy / pre-V1 comments (and missing or malformed ones) return {@code false}.
   */
  public static boolean hasV1Options(ObjectMapper mapper, TableMetadata table) {
    String comment = rawComment(table);
    if (comment == null || comment.isBlank()) {
      return false;
    }
    try {
      JsonNode options =
          mapper
              .readTree(comment)
              .path(TableCommentConstants.TOP_LEVEL_KEY)
              .path(TableCommentConstants.OPTIONS_KEY);
      return options.isObject();
    } catch (Exception e) {
      return false;
    }
  }
}
