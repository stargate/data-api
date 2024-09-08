package io.stargate.sgv2.jsonapi.util;

public class PrettyToStringBuilder {

  private static final String NEWLINE = System.lineSeparator();

  private final StringBuilder sb;
  private final int indent;
  private final boolean pretty;
  private final PrettyToStringBuilder parent;

  private boolean firstAppend = true;

  public PrettyToStringBuilder(Class<?> clazz, boolean pretty) {
    this(clazz, 1, pretty, null);
  }

  private PrettyToStringBuilder(Class<?> clazz, int indent, boolean pretty, PrettyToStringBuilder parent) {
    this.pretty = pretty;
    this.indent = indent;
    this.parent = parent;
    this.sb = parent == null ? new StringBuilder() : parent.sb;
    this.sb.append(clazz.getSimpleName()).append("{");
    newLine();
  }

  public PrettyToStringBuilder beginSubBuilder(Class<?> clazz) {
    return new PrettyToStringBuilder(clazz, indent + 1, pretty, this);
  }

  public PrettyToStringBuilder endSubBuilder() {
    if (parent != null) {indent();
    }
    sb.append("}");
    newLine();
    return parent;
  }

  public PrettyToStringBuilder append(String key, Object value) {
    if (!firstAppend) {
      popIfEndsWithNewline();
      sb.append(", ");
      newLine();
    }
    firstAppend = false;

    indent();
    sb.append(key).append("=");
    if (value instanceof PrettyPrintable pp) {
      pp.appendTo(this);
    }
    else {
      sb.append(value);
    }
    return this;
  }

  private boolean endsWithNewLine() {
    return sb.length() >= NEWLINE.length() && sb.substring(sb.length() - NEWLINE.length()).equals(NEWLINE);
  }

  private void popIfEndsWithNewline(){
    if (endsWithNewLine()) {
      sb.delete(sb.length() - NEWLINE.length(), sb.length());
    }
  }
  private void newLine() {
    if (pretty) {
      // when we use sub builders they end with a newline, easier to test and skip add if one is already there
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

//    sb.append("}");
    return sb.toString()  + "}";
  }
}
