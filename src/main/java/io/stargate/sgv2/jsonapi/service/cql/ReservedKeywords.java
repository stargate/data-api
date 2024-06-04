package io.stargate.sgv2.jsonapi.service.cql;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class ReservedKeywords {

  private static final Set<String> KEYWORDS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "SELECT",
                  "FROM",
                  "WHERE",
                  "AND",
                  "ENTRIES",
                  "FULL",
                  "INSERT",
                  "UPDATE",
                  "WITH",
                  "LIMIT",
                  "USING",
                  "USE",
                  "SET",
                  "BEGIN",
                  "UNLOGGED",
                  "BATCH",
                  "APPLY",
                  "TRUNCATE",
                  "DELETE",
                  "IN",
                  "CREATE",
                  "KEYSPACE",
                  "SCHEMA",
                  "COLUMNFAMILY",
                  "TABLE",
                  "MATERIALIZED",
                  "VIEW",
                  "INDEX",
                  "ON",
                  "TO",
                  "DROP",
                  "PRIMARY",
                  "INTO",
                  "ALTER",
                  "RENAME",
                  "ADD",
                  "ORDER",
                  "BY",
                  "ASC",
                  "DESC",
                  "ALLOW",
                  "IF",
                  "IS",
                  "GRANT",
                  "OF",
                  "REVOKE",
                  "MODIFY",
                  "AUTHORIZE",
                  "DESCRIBE",
                  "EXECUTE",
                  "NORECURSIVE",
                  "TOKEN",
                  "NULL",
                  "NOT",
                  "NAN",
                  "INFINITY",
                  "OR",
                  "REPLACE",
                  "DEFAULT",
                  "UNSET",
                  "MBEAN",
                  "MBEANS",
                  "FOR",
                  "RESTRICT",
                  "UNRESTRICT")));

  public static boolean isReserved(String text) {
    return KEYWORDS.contains(text.toUpperCase());
  }

  /** This class must not be instantiated as it only contains static methods. */
  private ReservedKeywords() {}
}
