package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import static io.stargate.sgv2.jsonapi.util.TableMetadataTestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultIndexMetadata;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.IndexDefs;
import io.stargate.sgv2.jsonapi.util.LoggerTestWrapper;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SuperShreddingIndexValidatorTest extends SuperShreddingBuilderTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SuperShreddingIndexValidatorTest.class);

  private SuperShreddingBinding buildBinding(SuperShreddingBuilder<?, ?> builder) {
    builder.build();
    return builder.binding();
  }

  private TableMetadata buildTableMetadata(SuperShreddingMetadataBuilder builder) {
    return (TableMetadata) builder.buildTableOnly();
  }

  @Nested
  class HealthyTables {

    @Test
    void allOptionalPresent() {
      var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
      var bindingBuilder = configAllOptional(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      var result = new SuperShreddingIndexValidator(binding).validate(table);

      assertThat(result.isHealthy()).as("all-optional table should be healthy").isTrue();
      assertThat(result.missingIndexes()).as("no missing indexes").isEmpty();
      assertThat(result.unexpectedIndexes()).as("no unexpected indexes").isEmpty();
      assertThat(result.presentIndexes())
          .as("present count matches expected")
          .hasSize(IndexDefs.REQUIRED.size() + 2); // +vector +lexical
    }

    @Test
    void noOptionalPresent() {
      var metadataBuilder = configNoOptional(SuperShreddingBuilder.metadata());
      var bindingBuilder = configNoOptional(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      var result = new SuperShreddingIndexValidator(binding).validate(table);

      assertThat(result.isHealthy()).as("no-optional table should be healthy").isTrue();
      assertThat(result.missingIndexes()).isEmpty();
      assertThat(result.presentIndexes()).hasSize(IndexDefs.REQUIRED.size());
    }

    @Test
    void vectorOnly() {
      var metadataBuilder = configVectorOnly(SuperShreddingBuilder.metadata());
      var bindingBuilder = configVectorOnly(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      var result = new SuperShreddingIndexValidator(binding).validate(table);

      assertThat(result.isHealthy()).isTrue();
      assertThat(result.presentIndexes()).hasSize(IndexDefs.REQUIRED.size() + 1);
    }

    @Test
    void lexicalOnly() {
      var metadataBuilder = configLexicalOnly(SuperShreddingBuilder.metadata());
      var bindingBuilder = configLexicalOnly(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      var result = new SuperShreddingIndexValidator(binding).validate(table);

      assertThat(result.isHealthy()).isTrue();
      assertThat(result.presentIndexes()).hasSize(IndexDefs.REQUIRED.size() + 1);
    }
  }

  @Nested
  class MissingRequiredIndexes {

    @Test
    void eachRequiredIndexMissing() {
      var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
      var bindingBuilder = configAllOptional(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      removeAllIndexes(table)
          .forEach(
              entry -> {
                var result =
                    new SuperShreddingIndexValidator(binding).validate(entry.tableMetadata());

                LOGGER.info(
                    "eachRequiredIndexMissing({}) - healthy:{}, missing:{}",
                    entry.indexName(),
                    result.isHealthy(),
                    result.missingIndexColumnNames());

                assertThat(result.isHealthy())
                    .as("table missing index %s should NOT be healthy", entry.indexName())
                    .isFalse();
                assertThat(result.missingIndexes())
                    .as("exactly one index missing when %s removed", entry.indexName())
                    .hasSize(1);
              });
    }

    @Test
    void allIndexesCleared() {
      var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
      var bindingBuilder = configAllOptional(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      var stripped = clearAllIndexes(table);
      var result = new SuperShreddingIndexValidator(binding).validate(stripped);

      assertThat(result.isHealthy()).isFalse();
      assertThat(result.missingIndexes())
          .as("all expected indexes should be missing")
          .hasSize(IndexDefs.REQUIRED.size() + 2);
      assertThat(result.presentIndexes()).isEmpty();
    }

    @Test
    void warnsOnMissingIndex() {
      var metadataBuilder = configNoOptional(SuperShreddingBuilder.metadata());
      var bindingBuilder = configNoOptional(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      var stripped = clearAllIndexes(table);

      try (var logWrapper = new LoggerTestWrapper(SuperShreddingIndexValidator.class)) {
        new SuperShreddingIndexValidator(binding).validate(stripped);

        assertThat(logWrapper.logMessages())
            .as("should log a warning about missing indexes")
            .anyMatch(msg -> msg.contains("missing SAI indexes"));
      }
    }
  }

  @Nested
  class MissingOptionalIndexes {

    @Test
    void vectorIndexMissingWhenExpected() {
      var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
      var bindingBuilder = configAllOptional(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      var vectorIndexName = IndexDefs.QUERY_VECTOR_VALUE.indexName(binding);
      var stripped = removeIndex(table, vectorIndexName);

      var result = new SuperShreddingIndexValidator(binding).validate(stripped);

      assertThat(result.isHealthy()).isFalse();
      assertThat(result.missingIndexColumnNames())
          .containsExactly(SuperShreddingMetadata.Names.QUERY_VECTOR_VALUE);
    }

    @Test
    void lexicalIndexMissingWhenExpected() {
      var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
      var bindingBuilder = configAllOptional(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      var lexicalIndexName = IndexDefs.QUERY_LEXICAL_VALUE.indexName(binding);
      var stripped = removeIndex(table, lexicalIndexName);

      var result = new SuperShreddingIndexValidator(binding).validate(stripped);

      assertThat(result.isHealthy()).isFalse();
      assertThat(result.missingIndexColumnNames())
          .containsExactly(SuperShreddingMetadata.Names.QUERY_LEXICAL_VALUE);
    }

    @Test
    void vectorIndexAbsentWhenNotExpected() {
      // Build a table WITH vector indexes, but validate with a binding that does NOT expect vector
      var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);

      var bindingBuilder = configNoOptional(SuperShreddingBuilder.metadata());
      var binding = buildBinding(bindingBuilder);

      var result = new SuperShreddingIndexValidator(binding).validate(table);

      // Healthy because the binding doesn't require vector/lexical
      assertThat(result.isHealthy()).isTrue();
      // But the vector and lexical indexes show up as unexpected
      assertThat(result.unexpectedIndexes()).hasSize(2);
    }
  }

  @Nested
  class UnexpectedIndexes {

    @Test
    void extraSAIIndex() {
      var metadataBuilder = configNoOptional(SuperShreddingBuilder.metadata());
      var bindingBuilder = configNoOptional(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      var bogusIndex =
          new DefaultIndexMetadata(
              table.getKeyspace(),
              table.getName(),
              CqlIdentifier.fromInternal("bogus_index"),
              IndexKind.CUSTOM,
              "\"bogus_column\"",
              Map.of("class_name", "StorageAttachedIndex", "target", "\"bogus_column\""));

      var withExtra = addIndex(table, bogusIndex);
      var result = new SuperShreddingIndexValidator(binding).validate(withExtra);

      assertThat(result.isHealthy()).as("still healthy -- extra indexes don't block").isTrue();
      assertThat(result.unexpectedIndexes()).hasSize(1);
      assertThat(result.unexpectedIndexNames()).containsExactly("bogus_index");
    }

    @Test
    void nonSAIIndex() {
      var metadataBuilder = configNoOptional(SuperShreddingBuilder.metadata());
      var bindingBuilder = configNoOptional(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      // A secondary index (not CUSTOM/SAI)
      var secondaryIndex =
          new DefaultIndexMetadata(
              table.getKeyspace(),
              table.getName(),
              CqlIdentifier.fromInternal("legacy_secondary"),
              IndexKind.COMPOSITES,
              "\"doc_json\"",
              Map.of("target", "\"doc_json\""));

      var withLegacy = addIndex(table, secondaryIndex);
      var result = new SuperShreddingIndexValidator(binding).validate(withLegacy);

      assertThat(result.isHealthy()).isTrue();
      assertThat(result.unexpectedIndexes()).hasSize(1);
      assertThat(result.unexpectedIndexNames()).containsExactly("legacy_secondary");
    }
  }

  @Nested
  class ResultAccessors {

    @Test
    void columnNamesForMissingIndexes() {
      var metadataBuilder = configNoOptional(SuperShreddingBuilder.metadata());
      var bindingBuilder = configNoOptional(SuperShreddingBuilder.metadata());
      var table = buildTableMetadata(metadataBuilder);
      var binding = buildBinding(bindingBuilder);

      var stripped = clearAllIndexes(table);
      var result = new SuperShreddingIndexValidator(binding).validate(stripped);

      assertThat(result.missingIndexColumnNames())
          .as("column names should match the required IndexDefs")
          .containsExactlyInAnyOrder(
              SuperShreddingMetadata.Names.EXIST_KEYS,
              SuperShreddingMetadata.Names.ARRAY_SIZE,
              SuperShreddingMetadata.Names.ARRAY_CONTAINS,
              SuperShreddingMetadata.Names.QUERY_BOOLEAN_VALUES,
              SuperShreddingMetadata.Names.QUERY_DOUBLE_VALUES,
              SuperShreddingMetadata.Names.QUERY_TEXT_VALUES,
              SuperShreddingMetadata.Names.QUERY_TIMESTAMP_VALUES,
              SuperShreddingMetadata.Names.QUERY_NULL_VALUES);
    }
  }
}
