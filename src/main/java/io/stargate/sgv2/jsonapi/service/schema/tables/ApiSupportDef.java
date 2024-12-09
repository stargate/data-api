package io.stargate.sgv2.jsonapi.service.schema.tables;

/**
 * Describes how the API supports a given data type.
 *
 * <p>Interface used for the internal API *Def types as opposed to the *Desc used for the public
 * API.
 *
 * <p>An interface and a record are used to support both basic situations and complex where it is
 * calculated on the fly.
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
   * Helper record to be used when the support can be determined at compile time, or easily cached
   */
  record Support(boolean createTable, boolean insert, boolean read, boolean filter)
      implements ApiSupportDef {

    public static final Support FULL = new Support(true, true, true, true);

    public static final Support NONE = new Support(false, false, false, false);
  }
}
