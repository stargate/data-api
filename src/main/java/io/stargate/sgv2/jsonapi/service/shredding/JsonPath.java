package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.Objects;

/**
 * Immutable value class used as key for entries in Shredded document representations: constructed
 * from nested path in input documents (during "shredding") to match "dot notation" path used for
 * query filtering (and possibly projection)
 *
 * <p>Internally path is simply expressed as a {@link String} where segments are separated by comma
 * ({@code "."}) and segments themselves are either:
 *
 * <ul>
 *   <li>Object property name (see below about escaping)
 *   <li>Array element index (see below for details)
 * </ul>
 *
 * <p>No escaping is used so path itself is ambiguous (segment "1" can mean either Array index #1 or
 * Object property "1") without context document, but will NOT be ambiguous when applied to specific
 * target document where context node's type (Array or Object) is known.
 *
 * <p>As a simple example consider following JSON document:
 *
 * <pre>
 *     { "name" : "Bob",
 *       "values" : [ 1, 2 ],
 *       "extra.stuff" : true
 *     }
 * </pre>
 *
 * would result in 5 different paths during shredding (no path for "root" node):
 *
 * <ul>
 *   <li>{@code "name"}
 *   <li>{@code "values"}
 *   <li>{@code "values.0"}
 *   <li>{@code "values.1"}
 *   <li>{@code "extra.stuff"}
 * </ul>
 *
 * <p>Instances can be sorted; sorting order uses underlying encoded path value.
 */
public final class JsonPath implements Comparable<JsonPath> {
  /** Encoded representation of the path as String. */
  private final String encodedPath;

  JsonPath(String encoded) {
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
  public static JsonPath from(String encoded) {
    return new JsonPath(encoded);
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
    return (o instanceof JsonPath) && encodedPath.equals(((JsonPath) o).encodedPath);
  }

  @Override
  public int compareTo(JsonPath o) {
    // Sorting by lexicographic (~= alphabetic) order of encoded path
    return this.encodedPath.compareTo(o.encodedPath);
  }

  /**
   * Builder that is used for efficient construction of {@link JsonPath}s when traversing a
   * (non-shredded) Document.
   */
  public static class Builder {
    /**
     * Base path to the context node (Array or Object) for which Builder is constructed; or {@code
     * null} for virtual Root object.
     */
    private final String basePath;

    /**
     * Path to currently traversed child of the context node, or {@code null} after construction but
     * before traversal of child nodes.
     */
    private String childPath;

    public Builder(String base) {
      this.basePath = base;
    }

    public Builder nestedValueBuilder() {
      // Must not be called unless we are pointing to a property or element:
      if (childPath == null) {
        throw new JsonApiException(ErrorCode.SHRED_INTERNAL_NO_PATH);
      }
      return new Builder(childPath);
    }

    public Builder property(String propName) {
      // Simple as we no longer apply escaping:
      childPath = (basePath == null) ? propName : (basePath + '.' + propName);
      return this;
    }

    public Builder index(int index) {
      // Indexes are easy as no escaping needed
      StringBuilder sb;

      if (basePath == null) { // root
        sb = new StringBuilder(6);
      } else {
        sb = new StringBuilder(basePath).append('.');
      }
      childPath = sb.append(index).toString();
      return this;
    }

    /**
     * Method used to construct path pointing to either the context node (Array, Object) -- if
     * called before traversal -- or to child node being traversed over.
     */
    public JsonPath build() {
      // "encoded" is null at point where Array or Object value is encountered but
      // contents not yet traversed. Path will therefore point to the Array/Object itself
      // and not element/property.
      if (childPath == null) {
        if (basePath == null) {
          // Means this is at root Object before any properties: could fail or build "empty":
          return new JsonPath("");
        }
        return new JsonPath(basePath);
      }
      return new JsonPath(childPath);
    }
  }
}
