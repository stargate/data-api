package io.stargate.sgv2.jsonapi.util;

/** An interface for objects that can be pretty-printed using the {@link PrettyToStringBuilder}. */
public interface PrettyPrintable {

  /**
   * Implementations will normally call {@link PrettyToStringBuilder#beginSubBuilder(Class)} ,
   * append tehir values, and then return the result of {@link
   * PrettyToStringBuilder#endSubBuilder()}.
   *
   * <p>If they do not create a sub-builder they are adding at the same level as the parent.
   *
   * <p>Example implementation:
   *
   * <pre>
   *   &#64;Override
   *   public String toString() {
   *     return toString(false);
   *   }
   *
   *   public String toString(boolean pretty) {
   *     return toString(new PrettyToStringBuilder(getClass(), pretty)).toString();
   *   }
   *
   *   public PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
   *     prettyToStringBuilder.append("keyspace", tableSchemaObject.tableMetadata.getKeyspace())
   *         .append("table", tableSchemaObject.tableMetadata.getName())
   *         .append("keyColumns", keyColumns)
   *         .append("nonKeyColumns", nonKeyColumns);
   *     return prettyToStringBuilder;
   *   }
   *
   *   &#64;Override
   *   public PrettyToStringBuilder appendTo(PrettyToStringBuilder prettyToStringBuilder) {
   *     var sb = prettyToStringBuilder.beginSubBuilder(getClass());
   *     return toString(sb).endSubBuilder();
   *   }
   * </pre>
   *
   * @param prettyToStringBuilder the builder to append to, or crete a sub-builder from
   * @return the builder to continue appending to
   */
  PrettyToStringBuilder appendTo(PrettyToStringBuilder prettyToStringBuilder);
}
