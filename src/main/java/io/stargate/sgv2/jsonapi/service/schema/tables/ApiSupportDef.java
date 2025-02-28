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
   * The type can be used as component for collection
   *
   * <p>E.G. list values, set values, map keys, map values.
   */
  Collection collection();

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

  /** The type can be used with update operators */
  Update update();

  default boolean isUnsupportedAny() {
    return isUnsupportedDDL() || isUnsupportedDML();
  }

  default boolean isUnsupportedDDL() {
    // TODO, should we call collectionSupport as part of DDL
    return !createTable();
  }

  default boolean isUnsupportedDML() {
    return !insert() || !read() || !filter();
  }

  /**
   * Record to represent if a dataType is supported as component for map/set/list.
   *
   * @param asListValue If the type can be created as value on a List.
   * @param asSetValue If the type can be created as value on a Set.
   * @param asMapKey If the type can be created as map key.
   * @param asMapValue If the type can be created as map value.
   */
  record Collection(boolean asListValue, boolean asSetValue, boolean asMapKey, boolean asMapValue) {
    public static final Collection FULL = new Collection(true, true, true, true);
    public static final Collection NONE = new Collection(false, false, false, false);
  }

  /**
   * Record to represent if a dataType is supported for update operations.
   *
   * @param set If the type can be used in a $set operation.
   * @param unset If the type can be used in a $unset operation.
   * @param push If the type can be used in a $push operation.
   * @param pullAll If the type can be used in a $pullAll operation.
   */
  record Update(boolean set, boolean unset, boolean push, boolean pullAll) {
    public static final Update PRIMITIVE = new Update(true, true, false, false);
    public static final Update FULL = new Update(true, true, true, true);
    public static final Update NONE = new Update(false, false, false, false);
  }

  /**
   * Helper record to be used when the support can be determined at compile time, or easily cached.
   */
  record Support(
      boolean createTable,
      Collection collection,
      boolean insert,
      boolean read,
      boolean filter,
      Update update)
      implements ApiSupportDef {

    public static final Support FULL =
        new Support(true, Collection.FULL, true, true, true, Update.PRIMITIVE);
    public static final Support NONE =
        new Support(false, Collection.NONE, false, false, false, Update.NONE);
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
  record Matcher(
      Boolean createTable,
      Collection collectionSupport,
      Boolean insert,
      Boolean read,
      Boolean filter)
      implements Predicate<ApiSupportDef> {

    public static final Matcher NO_MATCHES = new Matcher(null, null, null, null, null);

    /**
     * Returns a new matcher with the same values as this object, and the createTable value set to
     * the given value.
     */
    public Matcher withCreateTable(boolean createTable) {
      return new Matcher(createTable, collectionSupport, insert, read, filter);
    }

    /**
     * Returns a new matcher with the same values as this object, and the insert value set to the
     * given value.
     */
    public Matcher withInsert(boolean insert) {
      return new Matcher(createTable, collectionSupport, insert, read, filter);
    }

    /**
     * Returns a new matcher with the same values as this object, and the read value set to the
     * given value.
     */
    public Matcher withRead(boolean read) {
      return new Matcher(createTable, collectionSupport, insert, read, filter);
    }

    /**
     * Returns a new matcher with the same values as this object, and the filter value set to the
     * given value.
     */
    public Matcher withFilter(boolean filter) {
      return new Matcher(createTable, collectionSupport, insert, read, filter);
    }

    @Override
    public boolean test(ApiSupportDef apiSupportDef) {
      Objects.requireNonNull(apiSupportDef, "apiSupportDef must not be null");
      return (createTable == null || createTable == apiSupportDef.createTable())
          && (collectionSupport == null
              || (collectionSupport.asListValue == apiSupportDef.collection().asListValue
                  && collectionSupport.asSetValue == apiSupportDef.collection().asSetValue
                  && collectionSupport.asMapKey == apiSupportDef.collection().asMapKey
                  && collectionSupport.asMapValue == apiSupportDef.collection().asMapValue))
          && (insert == null || insert == apiSupportDef.insert())
          && (read == null || read == apiSupportDef.read())
          && (filter == null || filter == apiSupportDef.filter());
    }
  }
}
