package io.stargate.sgv2.jsonapi.service.schema.collections;

import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ProjectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An immutable representation of a document path composed of segments.
 *
 * <p>A document path is created from an encoded string (e.g., "pricing.price&amp;.usd"), where
 * segments are separated by dots ('.') and the escape character ('&amp;') allows literal dots or
 * ampersands within segments.
 *
 * <p>Use {@link #from(String)} to create an instance, {@link #getSegment(int)} to access the
 * segments, and {@link #encodeSegment(String)} to get the dot-separated path string.
 */
public final class DocumentPath implements Comparable<DocumentPath> {
  /** The full encoded path string - with escape character. */
  private final String encodedPath;

  /** The segments from the encoded path - without escape character. */
  private final List<String> segments;

  /** document path for document id */
  private static final DocumentPath DOC_ID = DocumentPath.from(DocumentConstants.Fields.DOC_ID);

  /** document path for vector embedding field */
  private static final DocumentPath VECTOR_EMBEDDING_FIELD =
      DocumentPath.from(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);

  /** document path for vector embedding text field */
  private static final DocumentPath VECTOR_EMBEDDING_TEXT_FIELD =
      DocumentPath.from(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);

  private DocumentPath(String originalPath, List<String> segments) {
    this.encodedPath = originalPath;
    this.segments = segments;
  }

  /** Accessor method to get the document path for document id */
  public static DocumentPath forDocId() {
    return DOC_ID;
  }

  /** Accessor method to get the document path for vector embedding field */
  public static DocumentPath forVectorEmbeddingField() {
    return VECTOR_EMBEDDING_FIELD;
  }

  /** Accessor method to get the document path for vector embedding text field */
  public static DocumentPath forVectorEmbeddingTextField() {
    return VECTOR_EMBEDDING_TEXT_FIELD;
  }

  /**
   * Factory method to create a DocumentPath from the given encoded path string. Handles escape
   * sequences where '&' escapes a dot or an ampersand.
   *
   * @param path the encoded path string (e.g., "pricing.price&.usd")
   * @return a new DocumentPath instance containing the decoded segments
   * @throws IllegalArgumentException if the escape sequence is invalid
   */
  public static DocumentPath from(String path) {
    Objects.requireNonNull(path, "path cannot be null");
    return new DocumentPath(path, decode(path));
  }

  /** Returns the number of segments in this document path. */
  public int getSegmentsSize() {
    return segments.size();
  }

  /** Returns the segment at the specified index. */
  public String getSegment(int index) {
    return segments.get(index);
  }

  /**
   * Verifies that the encoded path does not contain any lone ampersands. A lone ampersand is one
   * that is not followed by another ampersand or a dot, which are the only valid escape sequences.
   *
   * @param path the encoded path string to verify
   * @return the original path if it passes verification
   * @throws IllegalArgumentException if the path contains a lone ampersand
   */
  public static String verifyEncodedPath(String path) {
    Objects.requireNonNull(path, "path cannot be null");

    for (int i = 0; i < path.length(); ) {
      char ch = path.charAt(i++);
      // Check if this ampersand is part of a valid escape sequence
      if (ch == '&') {
        // Ampersand at the end of the string without anything to escape
        if (i >= path.length()) {
          throw new IllegalArgumentException(
              "The ampersand character '&' at position "
                  + (i - 1)
                  + " must be followed by either '&' or '.' to form a valid escape sequence. In path strings, '&' is used to escape '.' or '&' characters.");
        }

        // Ampersand not followed by '&' or '.'
        char nextChar = path.charAt(i++);
        if (nextChar != '&' && nextChar != '.') {
          throw new IllegalArgumentException(
              "The ampersand character '&' at position "
                  + (i - 2)
                  + " must be followed by either '&' or '.' to form a valid escape sequence. In path strings, '&' is used to escape '.' or '&' characters.");
        }
      }
    }

    return path;
  }

  /**
   * Encodes a path segment by escaping special characters.
   *
   * <p>The encoding process follows these steps:
   *
   * <ol>
   *   <li>
   *       <p>Scan the path segment character-by-character.
   *   <li>
   *       <p>If the character is {@code '.'}, escape it as {@code '&.'}.
   *   <li>
   *       <p>If the character is {@code '&'}, escape it as {@code '&&'}.
   *   <li>
   *       <p>All other characters remain unchanged.
   * </ol>
   *
   * <p>For example, the input {@code "item.weight"} results in: {@code "item&.weight"}.
   *
   * @param segment the raw path segment to encode.
   * @return the encoded path string.
   */
  public static String encodeSegment(String segment) {
    Objects.requireNonNull(segment, "Segment cannot be null");

    StringBuilder result = new StringBuilder();

    for (int i = 0; i < segment.length(); i++) {
      char ch = segment.charAt(i);

      // Escape '&' or '.' as '&&' or '&.'
      if (ch == '&' || ch == '.') {
        result.append('&');
      }
      result.append(ch);
    }

    return result.toString();
  }

  /**
   * Decodes a path string into its constituent segments based on custom rules.
   *
   * <p>The decoding process follows these steps:
   *
   * <ol>
   *   <li>
   *       <p>Scan the encoded path string character-by-character to build segments.
   *   <li>
   *       <p>If the scanned character is {@code '.'}, consider the current segment complete.
   *       <ul>
   *         <li>If the current segment is empty, an error is thrown (this handles cases where the
   *             path starts with a dot, ends with a dot, or contains consecutive dots).
   *         <li>Otherwise, the current segment is added to the list, and a new segment is started.
   *       </ul>
   *   <li>
   *       <p>If the scanned character is {@code '&'}, check for a following character:
   *       <ul>
   *         <li>
   *             <p>If there is no following character, an error is thrown.
   *         <li>
   *             <p>If the following character is either {@code '.'} or {@code '&'}, append that
   *             following character into current segment, and the escape is skipped.
   *         <li>
   *             <p>If it is any other character, an error is thrown (only {@code '&'} and {@code
   *             '.'} are allowed to be escaped).
   *       </ul>
   *   <li>
   *       <p>If the scanned character is neither {@code '&'} nor {@code '.'}, append it to the
   *       current segment.
   *   <li>
   *       <p>When the end of the expression is reached, consider the last segment complete.
   * </ol>
   *
   * <p>For example, the input {@code "pricing.price&.usd"} results in: {@code ["pricing",
   * "price.usd"]}.
   *
   * @param path the encoded path string.
   * @return a list of decoded segments.
   */
  private static List<String> decode(String path) {
    List<String> decodedSegments = new ArrayList<>();
    StringBuilder segment = new StringBuilder();

    for (int i = 0; i < path.length(); i++) {
      char ch = path.charAt(i);
      if (ch == '.') {
        // If the dot is encountered, the current segment is complete.
        // If the current segment is empty, error out.
        if (segment.isEmpty()) {
          throw ProjectionException.Code.UNSUPPORTED_PROJECTION_PATH.get(
              "unsupportedProjectionPath", path);
        }
        decodedSegments.add(segment.toString());
        segment.setLength(0); // reset the segment
      } else if (ch == '&') {
        // Escape character encountered
        if (i + 1 >= path.length()) {
          throw ProjectionException.Code.UNSUPPORTED_AMPERSAND_ESCAPE_USAGE.get(
              "unsupportedAmpersandEscape", path);
        }
        char next = path.charAt(i + 1);
        if (next == '.' || next == '&') {
          // Valid escape: append the next character and skip it
          segment.append(next);
          i++;
        } else {
          // Invalid escape: the next character is not either '.' or '&'
          throw ProjectionException.Code.UNSUPPORTED_AMPERSAND_ESCAPE_USAGE.get(
              "unsupportedAmpersandEscape", path);
        }
      } else {
        // Regular character: add to the current segment
        segment.append(ch);
      }
    }
    // Add the last segment and check if it's empty
    if (segment.isEmpty()) {
      throw ProjectionException.Code.UNSUPPORTED_PROJECTION_PATH.get(
          "unsupportedProjectionPath", path);
    }
    decodedSegments.add(segment.toString());
    return decodedSegments;
  }

  /**
   * Returns the string representation of the DocumentPath. By default, it returns the encoded form
   */
  @Override
  public String toString() {
    return encodedPath;
  }

  /**
   * Instead of simple alphabetic sorting of dotPath, do segment-aware to ensure parent/children are
   * sorted next to each other.
   */
  @Override
  public int compareTo(DocumentPath other) {
    final List<String> thisSegments = this.segments;
    final List<String> otherSegments = other.segments;

    for (int i = 0; i < Math.min(thisSegments.size(), otherSegments.size()); i++) {
      int diff = thisSegments.get(i).compareTo(otherSegments.get(i));
      if (diff != 0) {
        return diff;
      }
    }
    // If same prefix sort longer one after shorter one
    return Integer.compare(thisSegments.size(), otherSegments.size());
  }
}
