package io.stargate.sgv3.docsapi.service.shredding;

import java.util.Objects;

/**
 * Immutable value class used as key for entries in Shredded document representations: constructed
 * from either nested path in input documents (during "shredding"), or when "un-shredding" back to
 * Document from Shredded representations.
 *
 * <p>Internally path is simply expressed as a {@link String} where segments are separated by comma
 * ({@code "."}) and segments themselves are either:
 *
 * <ul>
 *   <li>Escaped Object property name (see below about escaping)
 *   <li>Decorated Array element index (see below for details)
 * </ul>
 *
 * <p>Array element indexes are enclosed in brackets, so the first element's segment would be
 * expressed as String {@code "[0]"}. Property names are included as is -- so property {@code
 * "name"} has segment {@code name} -- with the exception that due to need to distinguish
 * encoding-characters comma and opening bracket, escaping is needed. Backslash character {@code
 * '\\'} is used for escaping, preceding character to escape. Backslash itself also needs escaping.
 * This means that property name like {@code "a.b"} is encoded as {@code "a\\.b"}.
 *
 * <p>As a simple example consider following JSON document:
 *
 * <pre>
 *     { "name" : "Bob",
 *       "values" : [ 1, 2 ],
 *       "[extra.stuff]" : true
 *     }
 * </pre>
 *
 * would result in 5 different paths during shredding (no path for "root" node):
 *
 * <ul>
 *   <li>{@code "name"}
 *   <li>{@code "values"}
 *   <li>{@code "values.[0]"}
 *   <li>{@code "values.[1]"}
 *   <li>{@code "\\[extra\\.stuff]"}
 * </ul>
 */
public final class JSONPath {
  private static final JSONPath ROOT_PATH = new JSONPath("$");

  private final String encodedPath;

  JSONPath(String encoded) {
    encodedPath = Objects.requireNonNull(encoded, "Null not legal encoded path");
  }

  /**
   * Factory method that may be called to construct an instance from pre-encoded Path String (one
   * where character that must be escaped have been properly escpaed). Method does NOT verify that
   * escaping has been done correctly; caller is assumed to have ensured that.
   *
   * @param encoded
   * @return
   */
  public static JSONPath fromEncoded(String encoded) {
    return new JSONPath(encoded);
  }

  public static Builder rootBuilder() {
    return new Builder(null);
  }

  @Override
  public int hashCode() {
    return encodedPath.hashCode();
  }

  @Override
  public String toString() {
    return encodedPath;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    return (o instanceof JSONPath) && encodedPath.equals(((JSONPath) o).encodedPath);
  }

  /**
   * Builder that is used for efficient construction of {@link JSONPath}s when traversing a
   * (non-shredded) Document.
   */
  public static class Builder {
    private final String base;

    private String encoded;

    public Builder(String base) {
      this.base = base;
    }

    public Builder nestedValueBuilder() {
      // Must not be called unless we are pointing to a property or element:0
      if (encoded == null) {
        throw new IllegalStateException(
            "Path '" + build() + "' does not point to property or element");
      }
      return new Builder(encoded);
    }

    public Builder property(String propName) {
      // Properties trickier since need to escape '.' and '['
      StringBuilder sb;
      final int len = propName.length();

      if (base == null) { // root
        sb = new StringBuilder(len + 3);
      } else {
        sb = new StringBuilder(base.length() + len + 4).append(base).append('.');
      }

      for (int i = 0; i < len; ++i) {
        char c = propName.charAt(i);
        if (c == '.' || c == '[' || c == '\\') {
          sb.append('\\');
        }
        sb.append(c);
      }
      encoded = sb.toString();
      return this;
    }

    public Builder index(int index) {
      // Indexes are easy as no escaping needed
      StringBuilder sb;

      if (base == null) { // root
        sb = new StringBuilder(6);
      } else {
        sb = new StringBuilder(base).append('.');
      }
      encoded = sb.append('[').append(index).append(']').toString();
      return this;
    }

    public JSONPath build() {
      // We do allow constructing path for "whole" Object/Array too
      if (encoded == null) {
        return (base == null) ? ROOT_PATH : new JSONPath(base);
      }
      return new JSONPath(encoded);
    }
  }
}
