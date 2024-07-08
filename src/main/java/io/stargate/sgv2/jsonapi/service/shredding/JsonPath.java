package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
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

  /** Whether path points to an element of an array ({@code true}) or not. */
  private final boolean arrayElement;

  JsonPath(String encoded, boolean arrayElement) {
    encodedPath = Objects.requireNonNull(encoded, "Null not legal encoded path");
    this.arrayElement = arrayElement;
  }

  /** Factory method only used for testing. */
  public static JsonPath from(String encoded) {
    return from(encoded, false);
  }

  /** Factory method only used for testing. */
  public static JsonPath from(String encoded, boolean arrayElement) {
    return new JsonPath(encoded, arrayElement);
  }

  /**
   * Factory method for constructing root-level {@link Builder}: assumes Object context (root level
   * always implicit Object in Mongoose)
   */
  public static Builder rootBuilder() {
    return new Builder(null);
  }

  /**
   * @return Whether path points to an array element or not
   */
  public boolean isArrayElement() {
    return arrayElement;
  }

  /**
   * Convenience method for checking whether this path is {@code _id} (document primary key) or not
   */
  public boolean isDocumentId() {
    return DocumentConstants.Fields.DOC_ID.equals(encodedPath);
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
    if (o instanceof JsonPath) {
      JsonPath other = (JsonPath) o;
      return (arrayElement == other.arrayElement) && encodedPath.equals(other.encodedPath);
    }
    return false;
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

    /** Flag that indicates that the currently pointed-to path is to an array element */
    private final boolean inArray;

    public Builder(String base) {
      this(base, false);
    }

    Builder(String base, boolean inArray) {
      this.basePath = base;
      this.inArray = inArray;
    }

    /** Factory method used to construct a builder for elements of an Array value */
    public Builder nestedArrayBuilder() {
      // Must not be called unless we are pointing to a property or element:
      if (childPath == null) {
        throw ErrorCode.SERVER_INTERNAL_ERROR.toApiException(
            "Shredder path being built does not point to a property or element (basePath: '%s')",
            basePath);
      }
      return new Builder(childPath, true);
    }

    /** Factory method used to construct a builder for properties of an Object value */
    public Builder nestedObjectBuilder() {
      // Must not be called unless we are pointing to a property or element:
      if (childPath == null) {
        throw ErrorCode.SERVER_INTERNAL_ERROR.toApiException(
            "Shredder path being built does not point to a property or element (basePath: '%s')",
            basePath);
      }
      return new Builder(childPath, false);
    }

    /**
     * Accessor for checking whether path being built points directly to an array element
     * (regardless of whether contained in Array further up).
     *
     * @return True if the path being built points to an array element; false otherwise (Object
     *     property or root value)
     */
    public boolean isArrayElement() {
      return inArray;
    }

    public Builder property(String propName) {
      if (inArray) {
        throw new IllegalStateException(
            "Cannot add property '" + propName + "' when in array context: " + build());
      }
      childPath = (basePath == null) ? propName : (basePath + '.' + propName);
      return this;
    }

    public Builder index(int index) {
      if (!inArray) {
        throw new IllegalStateException(
            "Cannot add index (" + index + ") when not in array context: " + build());
      }
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
          return new JsonPath("", false);
        }
        return new JsonPath(basePath, inArray);
      }
      return new JsonPath(childPath, inArray);
    }
  }
}
