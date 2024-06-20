package io.stargate.sgv2.jsonapi.service.cql.builder;

import io.stargate.sgv2.jsonapi.service.cql.ColumnUtils;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.JsonTerm;
import java.util.Objects;

public final class BuiltCondition {

  public LHS lhs;

  public Predicate predicate;

  public JsonTerm jsonTerm;

  public BuiltCondition(LHS lhs, Predicate predicate, JsonTerm jsonTerm) {
    this.lhs = lhs;
    this.predicate = predicate;
    this.jsonTerm = jsonTerm;
  }

  public static BuiltCondition of(LHS lhs, Predicate predicate, JsonTerm jsonTerm) {
    return new BuiltCondition(lhs, predicate, jsonTerm);
  }

  public static BuiltCondition of(String columnName, Predicate predicate, JsonTerm jsonTerm) {
    return of(LHS.column(columnName), predicate, jsonTerm);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    // Append the LHS part of the condition
    if (lhs != null) {
      lhs.appendToBuilder(builder);
    } else {
      builder.append("null");
    }
    // Append the predicate part of the condition
    if (predicate != null) {
      builder.append(" ").append(predicate);
    } else {
      builder.append(" null");
    }
    // Append the JSON term part of the condition
    if (jsonTerm != null) {
      builder.append(" ").append(jsonTerm);
    } else {
      builder.append(" null");
    }
    return builder.toString();
  }

  /**
   * Represents the left hand side of a condition.
   *
   * <p>This is usually a column name, but technically can be:
   *
   * <ul>
   *   <li>a column name ("c = ...")
   *   <li>a specific element in a map column ("c[v] = ...")
   *   <li>a tuple of column name ("(c, d, e) = ...") (not supported)
   *   <li>the token of a tuple of column name ("TOKEN(c, d, e) = ...") (not supported)
   * </ul>
   */
  public abstract static class LHS {
    public static LHS column(String columnName) {
      return new ColumnName(columnName);
    }

    public static LHS mapAccess(String columnName, String key) {
      return new MapElement(columnName, key);
    }

    abstract void appendToBuilder(StringBuilder builder);

    static final class ColumnName extends LHS {
      private final String columnName;

      private ColumnName(String columnName) {
        this.columnName = columnName;
      }

      void appendToBuilder(StringBuilder builder) {
        builder.append(ColumnUtils.maybeQuote(columnName));
      }

      @Override
      public boolean equals(Object other) {
        if (other == this) {
          return true;
        } else if (other instanceof ColumnName) {
          ColumnName that = (ColumnName) other;
          return Objects.equals(this.columnName, that.columnName);
        } else {
          return false;
        }
      }

      @Override
      public int hashCode() {
        return Objects.hash(columnName);
      }
    }

    static final class MapElement extends LHS {
      private final String columnName;
      private final String key;

      MapElement(String columnName, String key) {
        this.columnName = columnName;
        this.key = key;
      }

      void appendToBuilder(StringBuilder builder) {
        builder.append(ColumnUtils.maybeQuote(columnName)).append("[?]");
      }

      @Override
      public boolean equals(Object other) {
        if (other == this) {
          return true;
        } else if (other instanceof MapElement) {
          MapElement that = (MapElement) other;
          return Objects.equals(this.columnName, that.columnName)
              && Objects.equals(this.key, that.key);
        } else {
          return false;
        }
      }

      @Override
      public int hashCode() {
        return Objects.hash(columnName, key);
      }
    }
  }
}
