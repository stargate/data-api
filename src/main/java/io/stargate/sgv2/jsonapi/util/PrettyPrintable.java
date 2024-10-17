package io.stargate.sgv2.jsonapi.util;

/** An interface for objects that can be pretty-printed using the {@link PrettyToStringBuilder}. */
public interface PrettyPrintable {

  /**
   * Implementations will normally just:
   *
   * <ol>
   *   <li>override the {@link #toString()} method to call {@link #toString(boolean)} with false
   *   <li>override the {@link #toString(PrettyToStringBuilder)} method to append the fields of the
   *       object as below, the builder will call the {@link
   *       PrettyPrintable#appendTo(PrettyToStringBuilder)} for any objects that implement this
   *       interface. </oi>
   *       <p>If they do not create a sub-builder they are adding at the same level as the parent.
   *       <p>Example implementation:
   *       <pre>
   *   &#64;Override
   *   public String toString() {
   *     return toString(false);
   *   }
   *
   *   public PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
   *     return prettyToStringBuilder
   *          .append("keyspace", tableSchemaObject.tableMetadata.getKeyspace())
   *         .append("table", tableSchemaObject.tableMetadata.getName())
   *         .append("keyColumns", keyColumns)
   *         .append("nonKeyColumns", nonKeyColumns);
   *   }
   *
   * </pre>
   */

  /**
   * Returns a string representation of the object, using the {@link PrettyToStringBuilder}
   *
   * @param pretty If <code>true</code> the string is pretty printed, using indents and carriage
   *     returns etc
   * @return A string representation of the object
   */
  default String toString(boolean pretty) {
    return toString(new PrettyToStringBuilder(getClass(), pretty)).toString();
  }

  /**
   * Appends the string representation of the object to the {@link PrettyToStringBuilder}, called by
   * the builder when building the string representation of the object in a hierarchy.
   *
   * @param prettyToStringBuilder The builder to append the string representation to
   * @return The builder returned from ending the sub-builder, for chaining
   */
  default PrettyToStringBuilder appendTo(PrettyToStringBuilder prettyToStringBuilder) {
    var sb = prettyToStringBuilder.beginSubBuilder(getClass());
    return toString(sb).endSubBuilder();
  }

  /**
   * Implement to append the fields for the object to the {@link PrettyToStringBuilder}, see class
   * documentation,
   *
   * @param prettyToStringBuilder The builder to append the string representation to
   * @return Return the builder returned from the last append() call, for chaining
   */
  PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder);
}
