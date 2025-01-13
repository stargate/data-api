package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownCqlIndexFunctionException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.util.defaults.Properties;
import io.stargate.sgv2.jsonapi.util.defaults.StringProperty;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/** Shared methods / constants for CQL SAI indexes. */
public abstract class CQLSAIIndex {

  /**
   * The common options from the {@link IndexMetadata#getOptions()} for SAI indexes. ApiIndex sub
   * classes can define their own options.
   */
  interface Options {
    // The class_name property from the index options, is required, we cannot guess it :)
    StringProperty CLASS_NAME = Properties.ofRequired("class_name");

    // The target property from the index options, is the name of the column the index is on
    // potentially prefixed with
    // the function to apply to the column. Required.
    StringProperty TARGET = Properties.ofRequired("target");
  }

  // the class name for SAI indexes, use the {@link #indexClassIsSai(String)} to check if an index
  // is an SAI index
  // it does a better job.
  public static final String SAI_CLASS_NAME = "StorageAttachedIndex";
  private static final String SAI_CLASS_NAME_ENDS_WITH = "." + SAI_CLASS_NAME;

  /**
   * Checks if the indexMetadata describes an SAI index.
   *
   * <p>We only support {@link IndexKind#CUSTOM} , these are from using CQL <code>
   * CREATE CUSTOM INDEX
   * </code> and we only support SAI index classes. Could get away with only checking the later but
   * double checking
   *
   * @param indexMetadata the index metadata to check
   * @return true if the index is an SAI index
   */
  static boolean isSAIIndex(IndexMetadata indexMetadata) {
    Objects.requireNonNull(indexMetadata, "indexMetadata must not be null");
    return indexMetadata.getKind() == IndexKind.CUSTOM
        && indexClassIsSai(Options.CLASS_NAME.getWithDefault(indexMetadata.getOptions()));
  }

  /**
   * Returns tue if the cql index class name is for an SAI index.
   *
   * <p>It is possible to pass the simple name when creating an index, <code>StorageAttachedIndex
   * </code> or a fully qualified name <code>org.apache.cassandra.index.sai.StorageAttachedIndex
   * </code> So we check the first, and if not that we check the class name ends with <code>
   * .StorageAttachedIndex</code>
   *
   * @param className the class name to check
   * @return true if the class name is for an SAI index
   */
  static boolean indexClassIsSai(String className) {
    return switch (className) {
      case SAI_CLASS_NAME -> true;
      case String s when s.endsWith(SAI_CLASS_NAME_ENDS_WITH) -> true;
      default -> false;
    };
  }

  /**
   * The target of the index is stored as a string in the indexing options in Cassandra 3.0+ See
   * {@link
   * com.datastax.oss.driver.internal.core.metadata.schema.parsing.TableParser#buildModernIndex(CqlIdentifier,
   * CqlIdentifier, AdminRow)} In the <code>system_schema.indexes</code> table the options can look
   * like:
   *
   * <pre>
   * options
   * ------------------------------------------------
   * {'class_name': 'StorageAttachedIndex', 'target': 'age'}
   * {'class_name': 'StorageAttachedIndex', 'target': '"myVectorColumn"'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'full(frozen_list_column)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(listcolumn)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(mapcolumn)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'keys(mapcolumn)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(mapcolumn)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(setcolumn)'}
   *
   * ------------------------------------------------
   * Then it is clear for Regex TARGET_REGEX. Valid Matches are:
   * keys(foo) -> keys(group1), foo(group2)
   * entries(bar) -> entries(group1), bar(group2)
   * values(some_column) -> values(group1), some_column(group2)
   * full(indexed_field) -> full(group1), index_field(group2)
   * values("capitalColumn") -> values(group1), "capitalColumn"(group2)
   * age -> not match, so resolve as simple columnName
   * "age" -> not match, so resolve as simple columnName
   *
   * ------------------------------------------------
   * If column is doubleQuoted in the original CQL index
   * we need to unquote to get the real column name
   * {'class_name': 'StorageAttachedIndex', 'target': '"myVectorColumn"'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values("listWithCapitalLetters")'}
   *
   * </pre>
   */
  private static final Pattern TARGET_REGEX =
      Pattern.compile("^(keys|entries|values|full)\\((.+)\\)$");

  private static final Pattern TWO_QUOTES = Pattern.compile("\"\"");
  private static final String QUOTE = "\"";

  /**
   * Parses the target from the IndexMetadata to extract the column name and the function if there
   * is one.
   *
   * <p>Index functions are used for indexing collections, see {@link #TARGET_REGEX} for the format
   * of the target
   *
   * <p>
   *
   * @param indexMetadata the index metadata to parse the target from
   * @return IndexTarget record, never null
   * @throws UnknownCqlIndexFunctionException - extracted the index function, but we do not know how
   *     to support it
   * @throws UnsupportedCqlIndexException - could not match the target for the index to extract the
   *     column and function
   */
  static IndexTarget indexTarget(IndexMetadata indexMetadata)
      throws UnknownCqlIndexFunctionException, UnsupportedCqlIndexException {
    Objects.requireNonNull(indexMetadata, "indexMetadata must not be null");

    // if the regex matches then the target is in the form "keys(foo)", "entries(bar)" etc
    // if not, then it must be a simple column name and implicitly its type is VALUES
    var target = indexMetadata.getTarget();
    Matcher matcher = TARGET_REGEX.matcher(target);
    String columnName;
    CqlIndexType cqlIndexType;
    if (matcher.matches()) {
      cqlIndexType = CqlIndexType.fromString(matcher.group(1));
      columnName = matcher.group(2);
    } else {
      columnName = target;
      cqlIndexType = CqlIndexType.VALUES;
    }

    // 1. In the case of a quoted column name the name in the target string
    // will be enclosed in quotes, which we need to unwrap.
    // 2. It may also include quote characters internally, escaped like so:
    // abc"def -> abc""def, then we need to un-escape as abc"def -> abc"def
    // to get the actual column name
    if (columnName.startsWith(QUOTE)) {
      columnName = StringUtils.substring(StringUtils.substring(columnName, 1), 0, -1);
      columnName = TWO_QUOTES.matcher(columnName).replaceAll(QUOTE);
    }

    return new IndexTarget(CqlIdentifier.fromInternal(columnName), cqlIndexType);
  }

  /** For internal to this package use only */
  public record IndexTarget(CqlIdentifier targetColumn, CqlIndexType cqlIndexType) {}

  public enum CqlIndexType {
    VALUES,
    KEYS,
    KEYS_AND_VALUES,
    FULL,
    SIMPLE;

    public String toString() {
      switch (this) {
        case KEYS:
          return "keys";
        case KEYS_AND_VALUES:
          return "entries";
        case FULL:
          return "full";
        case VALUES:
          return "values";
        case SIMPLE:
          return "";
        default:
          return "";
      }
    }

    public static CqlIndexType fromString(String s) {
      if ("".equals(s)) return SIMPLE;
      else if ("values".equals(s)) return VALUES;
      else if ("keys".equals(s)) return KEYS;
      else if ("entries".equals(s)) return KEYS_AND_VALUES;
      else if ("full".equals(s)) return FULL;

      throw new AssertionError("Unrecognized index target type " + s);
    }
  }
}
