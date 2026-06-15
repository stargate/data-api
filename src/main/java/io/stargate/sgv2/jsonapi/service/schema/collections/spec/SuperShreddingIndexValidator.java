package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.IndexDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.IndexDefs;
import io.stargate.sgv2.jsonapi.service.schema.tables.CQLSAIIndex;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.*;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates that a table identified as a super-shredding collection has the expected SAI indexes.
 *
 * <p>This is intentionally separate from {@link SuperShreddingTablePredicate}: the predicate
 * answers "is this a collection?" while this validator answers "is this collection healthy?". A
 * collection with missing indexes is still a collection -- just a degraded one that will fail
 * certain queries.
 *
 * <p>Call {@link #validate(TableMetadata)} after the predicate passes. The result describes what is
 * missing or unexpected so callers can decide whether to warn, error, or attempt repair.
 */
public class SuperShreddingIndexValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SuperShreddingIndexValidator.class);

  private final List<IndexDef> expectedIndexDefs;

  /**
   * Creates a validator that checks for the required indexes plus any optional indexes indicated by
   * the binding (vector, lexical).
   */
  public SuperShreddingIndexValidator(SuperShreddingBinding binding) {
    Objects.requireNonNull(binding, "binding must not be null");

    Stream.Builder<IndexDef> builder = Stream.builder();
    IndexDefs.REQUIRED.forEach(builder);
    if (binding.hasVector() && binding.isVectorDefined()) {
      builder.add(IndexDefs.QUERY_VECTOR_VALUE);
    }
    if (binding.hasLexical() && binding.isLexicalDefined()) {
      builder.add(IndexDefs.QUERY_LEXICAL_VALUE);
    }
    this.expectedIndexDefs = builder.build().toList();
  }

  /**
   * Validates the indexes on the given table metadata.
   *
   * @param tableMetadata the table metadata to validate, must not be null.
   * @return a result describing missing, present, and unexpected indexes.
   */
  public ValidationResult validate(TableMetadata tableMetadata) {
    Objects.requireNonNull(tableMetadata, "tableMetadata must not be null");

    Map<CqlIdentifier, IndexMetadata> actualIndexes = tableMetadata.getIndexes();

    // Build a set of the actual SAI index target columns for quick lookup.
    // We match on target column name because that is the stable part -- the index name
    // follows a convention (<table>_<column>) but we don't want to rely on it.
    Map<CqlIdentifier, IndexMetadata> actualByTargetColumn = new LinkedHashMap<>();
    for (IndexMetadata idx : actualIndexes.values()) {
      if (CQLSAIIndex.isSAIIndex(idx)) {
        try {
          var target = CQLSAIIndex.indexTarget(idx);
          actualByTargetColumn.put(target.targetColumn(), idx);
        } catch (Exception e) {
          // If we can't parse the target, skip it -- it will show up as unexpected
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "validate() - could not parse index target for index: {}, error: {}",
                idx.getName(),
                e.getMessage());
          }
        }
      }
    }

    List<IndexDef> missingIndexes = new ArrayList<>();
    List<IndexDef> presentIndexes = new ArrayList<>();

    for (IndexDef expected : expectedIndexDefs) {
      CqlIdentifier expectedColumn = expected.columnDef().name();
      if (actualByTargetColumn.containsKey(expectedColumn)) {
        presentIndexes.add(expected);
      } else {
        missingIndexes.add(expected);
      }
    }

    // Unexpected = SAI indexes on columns we don't expect
    Set<CqlIdentifier> expectedColumns = new LinkedHashSet<>();
    for (IndexDef def : expectedIndexDefs) {
      expectedColumns.add(def.columnDef().name());
    }
    List<IndexMetadata> unexpectedIndexes = new ArrayList<>();
    for (var entry : actualByTargetColumn.entrySet()) {
      if (!expectedColumns.contains(entry.getKey())) {
        unexpectedIndexes.add(entry.getValue());
      }
    }

    // clun: Non-SAI indexes are always unexpected in the context of a collection (to be validated ?)
    for (IndexMetadata idx : actualIndexes.values()) {
      if (!CQLSAIIndex.isSAIIndex(idx)) {
        unexpectedIndexes.add(idx);
      }
    }

    var result =
        new ValidationResult(
            Collections.unmodifiableList(missingIndexes),
            Collections.unmodifiableList(presentIndexes),
            Collections.unmodifiableList(unexpectedIndexes));

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "validate() - table: {}, missing: {}, present: {}, unexpected: {}",
          CqlIdentifierUtil.cqlIdentifierToMessageString(tableMetadata.getName()),
          result.missingIndexColumnNames(),
          result.presentIndexColumnNames(),
          result.unexpectedIndexNames());
    }

    if (!result.isHealthy() && LOGGER.isWarnEnabled()) {
      LOGGER.warn(
          "validate() - collection {} has missing SAI indexes on columns: {}",
          CqlIdentifierUtil.cqlIdentifierToMessageString(tableMetadata.getName()),
          result.missingIndexColumnNames());
    }

    return result;
  }

  /**
   * Result of validating the indexes on a super-shredding table.
   *
   * @param missingIndexes index definitions that should exist but don't.
   * @param presentIndexes index definitions that exist as expected.
   * @param unexpectedIndexes actual index metadata for indexes we didn't expect.
   */
  public record ValidationResult(
      List<IndexDef> missingIndexes,
      List<IndexDef> presentIndexes,
      List<IndexMetadata> unexpectedIndexes) {

    public boolean isHealthy() {
      return missingIndexes.isEmpty();
    }

    public List<String> missingIndexColumnNames() {
      return missingIndexes.stream().map(def -> def.columnDef().name().asInternal()).toList();
    }

    public List<String> presentIndexColumnNames() {
      return presentIndexes.stream().map(def -> def.columnDef().name().asInternal()).toList();
    }

    public List<String> unexpectedIndexNames() {
      return unexpectedIndexes.stream().map(idx -> idx.getName().asInternal()).toList();
    }
  }
}
