package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlOptionUtils.getStringIfPresent;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownCqlIndexFunctionException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/** Shared methods / constants for CQL SAI indexes. */
public abstract class CQLSAIIndex {

  /**
   * The common options from the {@link IndexMetadata#getOptions()} for SAI indexes. ApiIndex
   * subclasses can define their own options.
   */
  interface Options {
    // The class_name property from the index options, is required, we cannot guess it :)
    String CLASS_NAME = "class_name";

    // The target property from the index options, is the name of the column the index is on
    // potentially prefixed with the function to apply to the column. Required.
    String TARGET = "target";
  }

  // the class name for SAI indexes, use the {@link #indexClassIsSai(String)} to check if an index
  // is an SAI index
  // it does a better job.
  public static final String SAI_CLASS_NAME = "StorageAttachedIndex";
  private static final String SAI_CLASS_NAME_ENDS_WITH = "." + SAI_CLASS_NAME;

  /**
   * Checks if the indexMetadata describe an SAI index.
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
        && indexClassIsSai(getStringIfPresent(indexMetadata.getOptions(), Options.CLASS_NAME));
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
      case null, default -> false;
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
   * {'class_name': 'StorageAttachedIndex', 'target': 'age'}
   * {'class_name': 'StorageAttachedIndex', 'target': '"myVectorColumn"'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'full(frozen_list_column)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(listcolumn)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(mapcolumn)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'keys(mapcolumn)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(mapcolumn)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(setcolumn)'}
   * </pre>
   *
   * Then it is clear for Regex TARGET_REGEX. Detail in method below indexTarget(IndexMetadata
   * indexMetadata). Valid Matches are:
   *
   * <ul>
   *   <li>keys(foo) -> keys(group1), foo(group2)
   *   <li>entries(bar) -> entries(group1), bar(group2)
   *   <li>values(some_column) -> values(group1), some_column(group2)
   *   <li>full(indexed_field) -> full(group1), index_field(group2)
   *   <li>values("capitalColumn") -> values(group1), "capitalColumn"(group2)
   *   <li>age -> not match by groups, so resolve as simple columnName
   *   <li>"age" -> not match by groups, so resolve as simple columnName
   * </ul>
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

    // if the regex matches then the target is in the form "keys(foo)", "entries(bar)",
    // "values("foo")", "full("bar")" etc
    // if not, then it must be a simple column name.
    var target = indexMetadata.getTarget();
    Matcher matcher = TARGET_REGEX.matcher(target);
    String columnName;
    // TODO, discussion, API does not support frozen collections, so we could just ignore the full
    // index function
    // TODO, so full index function will be unknown, should we also have an enum mapping that
    ApiIndexFunction apiIndexFunction = null;
    if (matcher.matches()) {
      try {
        apiIndexFunction = ApiIndexFunction.fromCql(matcher.group(1).toLowerCase());
      } catch (UnknownCqlIndexFunctionException ignored) {
        // ignore full index, etc, that we do not support in ApiIndexFunction, apiIndexFunction will
        // be null
      }
      columnName = matcher.group(2);
    } else {
      columnName = target;
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
    return new IndexTarget(CqlIdentifier.fromInternal(columnName), apiIndexFunction);
  }

  /** For internal to this package use only */
  record IndexTarget(CqlIdentifier targetColumn, ApiIndexFunction indexFunction) {}
}
