package io.stargate.sgv2.jsonapi.service.schema.tables;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Describes how the API supports a given data type.
 *
 * <p>Interface used for the internal API *Def types as opposed to the *Desc used for the public
 * API.
 *
 * <p>An interface and a record are used to support both basic situations and complex where it is
 * calculated on the fly.
 *
 * <p>Use the Predicates and the {@link Matcher} to with functions on the {@link
 * ApiColumnDefContainer} to filter columns by level of support we have in the API.
 */
public interface ApiSupportDef {

  /**
   * The type can be used in a createTable statement.
   *
   * @return <code>true</code> if the type can be used in a createTable statement.
   */
  boolean createTable();

  /**
   * The type can be used in an insert command
   *
   * @return <code>true</code> if the type can be used in an insert command
   */
  boolean insert();

  /**
   * The type can be used in a read command
   *
   * @return <code>true</code> if the type can be used in a read command
   */
  boolean read();

  /**
   * The type can be used in a filter clause
   *
   * @return <code>true</code> if the type can be used in a filter clause
   */
  boolean filter();

  default boolean isUnsupportedAny() {
    return isUnsupportedDDL() || isUnsupportedDML();
  }

  default boolean isUnsupportedDDL() {
    return !createTable();
  }

  default boolean isUnsupportedDML() {
    return !insert() || !read() || !filter();
  }

  /**
   * Helper record to be used when the support can be determined at compile time, or easily cached.
   */
  record Support(boolean createTable, boolean insert, boolean read, boolean filter)
      implements ApiSupportDef {

    public static final Support FULL = new Support(true, true, true, true);

    public static final Support NONE = new Support(false, false, false, false);
  }

  /** Predicate to match for full support. */
  Predicate<ApiSupportDef> MATCH_FULL_SUPPORT = apiSupportDef -> !apiSupportDef.isUnsupportedAny();

  /** Predicate to match for no support. */
  Predicate<ApiSupportDef> MATCH_ANY_UNSUPPORTED = ApiSupportDef::isUnsupportedAny;

  /**
   * Matcher to filter when a {@link Predicate} is expected.
   *
   * <p>Create an instance or start with {@link Matcher#NO_MATCHES} and chain the methods to specify
   * the support you are looking for. All properties default to null, if null then the predicate wil
   * not test set to <code>false</code> or <code>tru</code> to test the value is as expected.
   *
   * @param createTable If non-null, will match objects where the createTable value is the same.
   * @param insert If non-null, will match objects where the insert value is the same.
   * @param read If non-null, will match objects where the read value is the same.
   * @param filter If non-null, will match objects where the filter value is the same.
   */
  record Matcher(Boolean createTable, Boolean insert, Boolean read, Boolean filter)
      implements Predicate<ApiSupportDef> {

    public static final Matcher NO_MATCHES = new Matcher(null, null, null, null);

    /**
     * Returns a new matcher with the same values as this object, and the createTable value set to
     * the given value.
     */
    public Matcher withCreateTable(boolean createTable) {
      return new Matcher(createTable, insert, read, filter);
    }

    /**
     * Returns a new matcher with the same values as this object, and the insert value set to the
     * given value.
     */
    public Matcher withInsert(boolean insert) {
      return new Matcher(createTable, insert, read, filter);
    }

    /**
     * Returns a new matcher with the same values as this object, and the read value set to the
     * given value.
     */
    public Matcher withRead(boolean read) {
      return new Matcher(createTable, insert, read, filter);
    }

    /**
     * Returns a new matcher with the same values as this object, and the filter value set to the
     * given value.
     */
    public Matcher withFilter(boolean filter) {
      return new Matcher(createTable, insert, read, filter);
    }

    @Override
    public boolean test(ApiSupportDef apiSupportDef) {
      Objects.requireNonNull(apiSupportDef, "apiSupportDef must not be null");
      return (createTable == null || createTable == apiSupportDef.createTable())
          && (insert == null || insert == apiSupportDef.insert())
          && (read == null || read == apiSupportDef.read())
          && (filter == null || filter == apiSupportDef.filter());
    }
  }
}
