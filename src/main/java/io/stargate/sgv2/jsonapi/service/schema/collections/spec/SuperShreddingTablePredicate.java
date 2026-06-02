package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.collect.Streams;
import io.stargate.sgv2.jsonapi.util.ColumnMetadataPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.Predicates.*;

/** Simple class that can check if table is a matching jsonapi table. */
public class SuperShreddingTablePredicate implements Predicate<TableMetadata> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SuperShreddingTablePredicate.class);

  private final List<ColumnMetadataPredicate> expectedOptionals;
  private final List<ColumnMetadataPredicate> strictMatch;

  public SuperShreddingTablePredicate(){
    this(false, false, false);
  }

  public SuperShreddingTablePredicate(boolean strict, boolean expectVector, boolean expectLexical ){

    List<ColumnMetadataPredicate> local = new ArrayList<>();
    if(expectVector){
      local.add(SuperShreddingMetadata.Predicates.QUERY_VECTOR_VALUE);
    }
    if(expectLexical){
      local.add(SuperShreddingMetadata.Predicates.QUERY_LEXICAL_VALUE);
    }
    this.expectedOptionals = Collections.unmodifiableList(local);

    this.strictMatch = strict ?
            Stream.concat(SuperShreddingMetadata.Predicates.REQUIRED.stream(), expectedOptionals.stream()).toList()
            :
            null;
  }

  /**
   * Tests if the given table is a valid jsonapi table.
   *
   * @param tableMetadata the table
   * @return Returns true only if all the columns in the table correspond to the data-api table
   *     schema.
   */
  @Override
  public boolean test(TableMetadata tableMetadata) {

    if (null == tableMetadata) {
      return false;
    }

    List<ColumnMetadataPredicate> failingPredicates;
    List<ColumnMetadata> unexpectedColumns;

    failingPredicates = allFailingPredicates(SuperShreddingMetadata.Predicates.PARTITION_KEY, tableMetadata.getPartitionKey());
    if (!failingPredicates.isEmpty()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("test() - partition key has missing column, failingPredicates: {}", failingPredicates);
      }
      return false;
    }

    unexpectedColumns = allUnexpectedColumns(SuperShreddingMetadata.Predicates.PARTITION_KEY, tableMetadata.getPartitionKey());
    if (!unexpectedColumns.isEmpty()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("test() - partition key unexpected column, unexpectedColumns: {}", unexpectedColumns);
      }
      return false;
    }

    if (!tableMetadata.getClusteringColumns().isEmpty()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("test() - clustering columns non empty, clusteringColumns: {}", tableMetadata.getClusteringColumns().keySet());
      }
      return false;
    }

    failingPredicates = allFailingPredicates(SuperShreddingMetadata.Predicates.REQUIRED,  tableMetadata.getColumns().values());
    if (!failingPredicates.isEmpty()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("test() - required columns missing, failingPredicates: {}", failingPredicates);
      }
      return false;
    }

    failingPredicates = allFailingPredicates(expectedOptionals,  tableMetadata.getColumns().values());
    if (!failingPredicates.isEmpty()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("test() - expected optional columns missing, failingPredicates: {}", failingPredicates);
      }
      return false;
    }

    if (strictMatch != null){
      var allTableColumns = Streams.concat(
              tableMetadata.getPartitionKey().stream(),
              tableMetadata.getClusteringColumns().keySet().stream(),
              tableMetadata.getColumns().values().stream()).toList();
      unexpectedColumns = allUnexpectedColumns(strictMatch,  allTableColumns);
      if (!unexpectedColumns.isEmpty()) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("test() - using strict mode, unexpected columns in all table columns, unexpectedColumns: {}", unexpectedColumns);
        }
        return false;
      }
    }

    return true;
  }
}
