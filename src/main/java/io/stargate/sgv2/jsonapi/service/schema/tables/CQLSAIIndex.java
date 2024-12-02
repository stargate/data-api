package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.util.Strings;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownCqlIndexFunctionException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.util.defaults.Properties;
import io.stargate.sgv2.jsonapi.util.defaults.StringProperty;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
   * Checks if the indexMetadata deacribes an SAI index.
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
   * {'class_name': 'StorageAttachedIndex', 'target': 'country'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(array_contains)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(array_size)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(exist_keys)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_bool_values)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_dbl_values)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values(query_null_values)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_text_values)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_timestamp_values)'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'comment_vector'}
   * {'class_name': 'StorageAttachedIndex', 'similarity_function': 'cosine', 'target': 'my_vector'}
   * ------------------------------------------------
   * If column is doubleQuoted in the original CQL index
   * {'class_name': 'StorageAttachedIndex', 'target': '"Age"'}
   * {'class_name': 'StorageAttachedIndex', 'target': 'values("Age")'}
   * </pre>
   *
   * The target can just be the name of the column, or the name of the column in parentheses
   * prefixed by the index type for a map type: values(column), keys(column), and entries(column)
   * see https://docs.datastax.com/en/cql/hcd-1.0/develop/indexing/sai/collections.html
   *
   * <p>The Reg Exp below will match:
   *
   * <ul>
   *   <li>'monkeys': group 1 - 'monkeys', group 2 - null
   *   <li>'values(monkeys)': group 1 - 'values' group 2 - 'monkeys'
   *   <li>'"Monkeys": group 1 - '"Monkeys"', group 2 - null
   *   <li>'values("Monkeys")': group 1 - 'values' group 2 - '"monkeys"'
   * </ul>
   */
  private static Pattern INDEX_TARGET_PATTERN =
      Pattern.compile("^(\"?\\w+\"?)?(?:\\((\"?\\w+\"?)\\))?$");

  /**
   * Parses the target from the IndexMetadata to extract the column name and the function if there
   * is one.
   *
   * <p>Index functions are used for indexing collections, see {@link #INDEX_TARGET_PATTERN} for the
   * format of the target
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

    var target = CQLSAIIndex.Options.TARGET.getWithDefault(indexMetadata.getOptions());
    Matcher matcher = INDEX_TARGET_PATTERN.matcher(target);
    if (!matcher.matches()) {
      throw new UnsupportedCqlIndexException(
          "Could not parse index target: '" + target + "'", indexMetadata);
    }

    String columnName;
    String functionName;
    if (matcher.group(2) == null) {
      columnName = matcher.group(1);
      functionName = null;
    } else {
      columnName = matcher.group(2);
      functionName = matcher.group(1);
    }
    // At this point, after matcher, we may get a columnName doubleQuoted string from the index
    // target
    // E.G. 'target': 'values("a_list_column")' -> "a_list_column", 'target':
    // 'values("a_regular_column")' -> "a_regular_column"
    // 1. We can NOT use fromInternal which will keep the double quote in the identifier
    // 2. We can NOT use fromCQL, since it will lowercase the unquoted string, E.G. 'BigApple' ->
    // bigapple
    // So we should just stripe the doubleQuote if needed
    columnName =
        Strings.isDoubleQuoted(columnName) ? Strings.unDoubleQuote(columnName) : columnName;

    return functionName == null
        ? new IndexTarget(CqlIdentifier.fromInternal(columnName), null)
        : new IndexTarget(
            CqlIdentifier.fromInternal(columnName), ApiIndexFunction.fromCql(functionName));
  }

  /** For internal to this package use only */
  protected record IndexTarget(CqlIdentifier targetColumn, ApiIndexFunction indexFunction) {}
}
