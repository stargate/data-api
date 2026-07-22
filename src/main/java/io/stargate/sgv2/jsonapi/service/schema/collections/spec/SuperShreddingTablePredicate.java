package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import static io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.Predicates.*;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.collect.Streams;
import io.stargate.sgv2.jsonapi.exception.ErrorFormatters;
import io.stargate.sgv2.jsonapi.util.ColumnMetadataPredicate;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Predciate to test if a {@link TableMetadata} is a valid Collection table, on that has the super
 * shredding table schema.
 *
 * <p>This class is designed to build via {@link SuperShreddingBuilder#predicate()} and the builder
 * it provides, so that there is shared logic between the builders that are used to create the super
 * shredding table and the predicate used to test for it. See {@link
 * SuperShreddingPredicateBuilder}.
 *
 * <p>Uses the shared abstract definition of super shredding in {@link SuperShreddingMetadata}
 *
 * <p><b>Note:</b> How we create the statements for, predicate to test for, and test data to use
 * with code that uses a super shredding table starts with the {@link SuperShreddingBuilder} class
 * which has some slightly complex tests around it.
 *
 * <p>This class used to be called <code>CollectionTableMatcher</code>
 *
 * <p><b>NOTE:</b> As of June 2026, there is no check the indexes are valid, this will be future
 * work (aaron)
 */
public class SuperShreddingTablePredicate implements Predicate<TableMetadata> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SuperShreddingTablePredicate.class);

  private final SuperShreddingBinding superShreddingBinding;
  private final List<ColumnMetadataPredicate> expectedOptionals;

  // when non null, this is the list of predicates that defines the columns that are ONLY allowed to
  // exist
  private final List<ColumnMetadataPredicate> strictMatch;

  // A def that represents the rules used by the old `CollectionTableMatcher`
  private static final SuperShreddingBinding BACKWARDS_COMPAT =
      new SuperShreddingBinding(null, null, false, 0, null, null, false, null);

  /**
   * Visible for backwards compatibility.
   *
   * <p>Creates an instance that does not use strict mode, and does not check for optional columns.
   */
  public SuperShreddingTablePredicate() {
    this(false, BACKWARDS_COMPAT);
  }

  /**
   * Creates an instance that checks if the table matches the super shredding definition passed in.
   *
   * @param strict if true, the predicate will error if unexpected columns are found.
   * @param superShreddingBinding the super shredding definition to use for the predicate, build via
   *     builders.
   */
  SuperShreddingTablePredicate(boolean strict, SuperShreddingBinding superShreddingBinding) {

    this.superShreddingBinding =
        Objects.requireNonNull(superShreddingBinding, "superShreddingDef must not be null");

    List<ColumnMetadataPredicate> optionals = new ArrayList<>();
    if (superShreddingBinding.hasVector()) {
      optionals.add(SuperShreddingMetadata.Predicates.QUERY_VECTOR_VALUE);
    }
    if (superShreddingBinding.hasLexical()) {
      optionals.add(SuperShreddingMetadata.Predicates.QUERY_LEXICAL_VALUE);
    }
    this.expectedOptionals = Collections.unmodifiableList(optionals);

    this.strictMatch =
        strict
            ? Stream.concat(
                    SuperShreddingMetadata.Predicates.REQUIRED.stream(), expectedOptionals.stream())
                .toList()
            : null;
  }

  /**
   * Tests if the given table is a valid super shredding.
   *
   * @param tableMetadata the table to test
   * @return true if the table is a valid super shredding, false otherwise.
   */
  @Override
  public boolean test(TableMetadata tableMetadata) {

    // The trace messages are used in the testing to confirm we are failing the way the test expects
    if (null == tableMetadata) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("test() - tableMetadata is null");
      }
      return false;
    }

    List<ColumnMetadataPredicate> failingPredicates;
    List<ColumnMetadata> unexpectedColumns;

    // STEP 1 - Partition Key, in strict or not, must be exactly as we expect

    failingPredicates =
        allFailingPredicates(
            SuperShreddingMetadata.Predicates.PARTITION_KEY, tableMetadata.getPartitionKey());
    if (!failingPredicates.isEmpty()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(failedPredicates("partition key missing", failingPredicates));
      }
      return false;
    }

    unexpectedColumns =
        allUnexpectedColumns(
            SuperShreddingMetadata.Predicates.PARTITION_KEY, tableMetadata.getPartitionKey());
    if (!unexpectedColumns.isEmpty()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(unexpectedColumns("unexpected columns in partition key", unexpectedColumns));
      }
      return false;
    }

    // STEP 2 - Clustering Keys, in strict or not, must be exactly as we expect which is empty

    if (!tableMetadata.getClusteringColumns().isEmpty()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            unexpectedColumns(
                "unexpected columns in clustering key",
                tableMetadata.getClusteringColumns().keySet()));
      }
      return false;
    }

    // STEP 3 - Columns - Check for required and optional based on the Def (set in ctor)

    failingPredicates =
        allFailingPredicates(
            SuperShreddingMetadata.Predicates.REQUIRED, tableMetadata.getColumns().values());
    if (!failingPredicates.isEmpty()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(failedPredicates("required columns missing", failingPredicates));
      }
      return false;
    }

    failingPredicates =
        allFailingPredicates(expectedOptionals, tableMetadata.getColumns().values());
    if (!failingPredicates.isEmpty()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(failedPredicates("optional columns missing", failingPredicates));
      }
      return false;
    }

    // STEP 4 - Strict Columns - If set, then we can only have the expected columns

    if (strictMatch != null) {
      var allTableColumns =
          Streams.concat(
                  tableMetadata.getPartitionKey().stream(),
                  tableMetadata.getClusteringColumns().keySet().stream(),
                  tableMetadata.getColumns().values().stream())
              .toList();
      unexpectedColumns = allUnexpectedColumns(strictMatch, allTableColumns);
      if (!unexpectedColumns.isEmpty()) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(unexpectedColumns("unexpected columns in strict mode", unexpectedColumns));
        }
        return false;
      }
    }

    return true;
  }

  private static String failedPredicates(
      String failure, Collection<ColumnMetadataPredicate> failingPredicates) {

    // Rely on the toString in the ColumnMetadataPredicate
    var names =
        failingPredicates.stream()
            .sorted(ColumnMetadataPredicate.IDENTIFIER_COMPARATOR)
            .map(Object::toString)
            .collect(Collectors.joining(", "));
    return failureMessages(failure, names);
  }

  private static String unexpectedColumns(String failure, Collection<ColumnMetadata> unexpected) {

    var names =
        unexpected.stream()
            .sorted(CqlIdentifierUtil.COLUMN_METADATA_COMPARATOR)
            .map(ErrorFormatters::errFmt)
            .collect(Collectors.joining(", "));
    return failureMessages(failure, names);
  }

  private static String failureMessages(String failure, String names) {
    // e.g. "required columns missing, columns: exist_keys, key"
    return "test() - " + failure + ", columns: " + names;
  }
}
