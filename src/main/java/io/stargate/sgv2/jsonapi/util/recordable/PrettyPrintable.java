package io.stargate.sgv2.jsonapi.util.recordable;

import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer;
import java.util.List;

/**
 * An interface for objects that can be pretty-printed using the {@link PrettyPrintableRecorder}.
 * Implementations will normally just:
 *
 * <ol>
 *   <li>override the {@link #toString()} method to call {@link #toString(boolean)} with false
 *   <li>override the {@link #toString(PrettyPrintableRecorder)} method to append the fields of the
 *       object as below, the builder will call the {@link
 *       PrettyPrintable#appendTo(PrettyPrintableRecorder)} for any objects that implement this
 *       interface. </oi>
 *       <p>If they do not create a sub-builder they are adding at the same level as the parent.
 *       <p>Example implementation:
 *       <pre>
 *   &#64;Override
 *   public String toString() {
 *     return toString(false);
 *   }
 *
 *   public DataRecorder recordTo(DataRecorder dataRecorder) {
 *     return prettyToStringBuilder
 *          .append("keyspace", tableSchemaObject.tableMetadata.getKeyspace())
 *         .append("table", tableSchemaObject.tableMetadata.getName())
 *         .append("keyColumns", keyColumns)
 *         .append("nonKeyColumns", nonKeyColumns);
 *   }
 *
 * </pre>
 */
public interface PrettyPrintable {

  /**
   * Returns a string representation of the object, using the {@link PrettyPrintableRecorder}
   *
   * @param pretty If <code>true</code> the string is pretty printed, using indents and carriage
   *     returns etc
   * @return A string representation of the object
   */
  //  default String toString(boolean pretty) {
  //    return prettyPrint(this, pretty);
  //  }
  //
  //  static String prettyPrint(Object object) {
  //    return (object instanceof Recordable pp) ? pp.toString(true) : object.toString();
  //  }

  static String pprint(Recordable recordable) {
    return pprint(recordable, true);
  }

  static String pprint(Recordable recordable, boolean prettyPrint) {
    return recordable
        .recordTo(new PrettyPrintableRecorder(recordable.getClass(), prettyPrint))
        .toString();
  }

  /**
   * Appends the string representation of the object to the {@link PrettyPrintableRecorder}, called
   * by the builder when building the string representation of the object in a hierarchy.
   *
   * @param prettyToStringBuilder The builder to append the string representation to
   * @return The builder returned from ending the sub-builder, for chaining
   */
  //  default PrettyPrintableRecorder appendTo(PrettyPrintableRecorder prettyToStringBuilder) {
  //    var sb = prettyToStringBuilder.beginSubRecorder(getClass());
  //    return toString(sb).endSubBuilder();
  //  }

  /**
   * Implement to append the fields for the object to the {@link PrettyPrintableRecorder}, see class
   * documentation,
   *
   * @param prettyToStringBuilder The builder to append the string representation to
   * @return Return the builder returned from the last append() call, for chaining
   */
  //  DataRecorder recordTo(DataRecorder dataRecorder);

  /**
   * Helper used to print the objects when testing, for example the {@link JsonNamedValueContainer}
   * and the fixture types.
   *
   * <p>Useful because there tests have a lot of context with them, example of the sort of output:
   *
   * <pre>
   *   INFO  [main] 2024-09-09 13:24:01,849 WriteableTableRowBuilderTest.java:35 - allColumns:
   * fixture=AllColumns{
   * 	table=KeyValueTwoPrimaryKeys,
   * 	identifiers=UnquotedLCaseAlphaNum,
   * 	data=MaxNumericData,
   * 	setKeys=[DefaultColumnMetadata@d19ec42(keyspace1921003048.table782588238.key0 text), DefaultColumnMetadata@fffa2d42(keyspace1921003048.table782588238.col1 text)],
   * 	setNonKeyColumns=[DefaultColumnMetadata@fffa3103(keyspace1921003048.table782588238.col2 text)], }
   * container=UnorderedJsonNamedValueContainer{
   * 	key0=JsonNamedValue{
   * 		name=key0,
   * 		value=JsonLiteral{type=STRING, value(String)=text}		    },
   * 	col2=JsonNamedValue{
   * 		name=col2,
   * 		value=JsonLiteral{type=STRING, value(String)=text}    },
   * 	col1=JsonNamedValue{
   * 		name=col1,
   * 		value=JsonLiteral{type=STRING, value(String)=text}    }
   * }
   * </pre>
   */
  class PrettyPrintableRecorder extends Recordable.DataRecorder {

    private static final String NEWLINE = System.lineSeparator();

    private final StringBuilder sb;
    private final int indent;
    private final boolean pretty;
    private boolean firstAppend = true;

    public PrettyPrintableRecorder(Class<?> clazz, boolean pretty) {
      this(clazz, 1, pretty, null);
    }

    private PrettyPrintableRecorder(
        Class<?> clazz, int indent, boolean pretty, PrettyPrintableRecorder parent) {
      super(clazz, parent);
      this.pretty = pretty;
      this.indent = indent;

      this.sb = parent == null ? new StringBuilder() : parent.sb;

      this.sb.append(className(clazz)).append("{");
      newLine();
    }

    @Override
    public Recordable.DataRecorder beginSubRecorder(Class<?> clazz) {
      return new PrettyPrintableRecorder(clazz, indent + 1, pretty, this);
    }

    @Override
    public Recordable.DataRecorder endSubRecorder() {
      if (parent != null) {
        indent();
      }
      sb.append("}");
      newLine();
      return parent;
    }

    @Override
    public Recordable.DataRecorder append(String key, Object value) {
      if (!firstAppend) {
        popIfEndsWithNewline();
        sb.append(", ");
        newLine();
      }
      firstAppend = false;

      indent();
      sb.append(key).append("=");
      if (value instanceof Recordable recordable) {
        recordable.recordToSubRecorder(this);
      } else if (value instanceof List<?> list) {
        sb.append("[");
        boolean first = true;
        for (Object item : list) {
          if (!first) {
            sb.append(", ");
          }
          if (item instanceof Recordable recordable) {
            recordable.recordToSubRecorder(this);
          } else {
            sb.append(item);
          }
          first = false;
        }
        sb.append("]");
      } else {
        sb.append(value);
      }
      return this;
    }

    private boolean endsWithNewLine() {
      return sb.length() >= NEWLINE.length()
          && sb.substring(sb.length() - NEWLINE.length()).equals(NEWLINE);
    }

    private void popIfEndsWithNewline() {
      if (endsWithNewLine()) {
        sb.delete(sb.length() - NEWLINE.length(), sb.length());
      }
    }

    private void newLine() {
      if (pretty) {
        // when we use sub builders they end with a newline, easier to test and skip add if one is
        // already there
        if (!endsWithNewLine()) {
          sb.append(System.lineSeparator());
        }
      }
    }

    private void indent() {
      if (pretty) {
        sb.append("\t".repeat(indent));
      }
    }

    @Override
    public String toString() {
      if (parent != null) {
        throw new IllegalStateException("Cannot call toString on a sub-builder");
      }
      // do not append to the StringBuilder, because when debugging this gets called a lot.
      return sb.toString() + "}";
    }
  }
}
