package io.stargate.sgv2.jsonapi.service.operation.model.impl.builder;

import io.stargate.sgv2.api.common.cql.ColumnUtils;
import java.util.Objects;

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
public abstract class ConditionLHS {

  public static ConditionLHS column(String columnName) {
    return new ColumnName(columnName);
  }

  public static ConditionLHS mapAccess(String columnName, String key) {
    return new MapElement(columnName, key);
  }

  // TODO: COMMENT!
  public abstract void appendToBuilder(StringBuilder builder);

  static final class ColumnName extends ConditionLHS {
    private final String columnName;

    private ColumnName(String columnName) {
      this.columnName = columnName;
    }

    @Override
    public void appendToBuilder(StringBuilder builder) {
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

  // TODO: COMMENT!
  static final class MapElement extends ConditionLHS {
    private final String columnName;
    private final String key;

    MapElement(String columnName, String key) {
      this.columnName = columnName;
      this.key = key;
    }

    @Override
    public void appendToBuilder(StringBuilder builder) {
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
