package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import io.stargate.sgv2.jsonapi.TestConstants;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingCQL.collapseWhitespace;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testing that when we build TableMetadata for super shredding table, it matches the expected CQL statement
 * from
 */
public class SuperShreddingMetadataBuilderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SuperShreddingMetadataBuilderTest.class);


    private final TestConstants TEST_CONSTANTS = new TestConstants();

    @Test
    public void createTableAllOptional() {

        var metadataBuilder = SuperShreddingCQLBuilder.metadata()
                .withKeyspace(TEST_CONSTANTS.KEYSPACE_IDENTIFIER.keyspace())
                .withCollection(TEST_CONSTANTS.COLLECTION_IDENTIFIER.table())
                .withVector(1024, "cosine", "OTHER")
                .withLexical("standard");

        var cqlBuilder = SuperShreddingCQLBuilder.cql()
                .withKeyspace(TEST_CONSTANTS.KEYSPACE_IDENTIFIER.keyspace())
                .withCollection(TEST_CONSTANTS.COLLECTION_IDENTIFIER.table())
                .withVector(1024, "cosine", "OTHER")
                .withLexical("standard");

        var metadataComponents = metadataBuilder.build();
        var cqlComponents = cqlBuilder.build();

        for (var cqlComponent : cqlComponents) {

            var metadataComponent = metadataComponents.stream()
                    .filter(c -> c.identifier().equals(cqlComponent.identifier()))
                    .findFirst()
                    .orElse(null);

            assertThat(metadataComponent)
                    .as("Metadata component for '%s' should not be null", cqlComponent.identifier())
                    .isNotNull();

            var expectedCql = collapseWhitespace(cqlComponent.value());
            var actualCql = collapseWhitespace(metadataComponent.value().describe(false ));

            LOGGER.info("createTableAllOptional() - cqlComponent: {}, expectedCql: {}", cqlComponent.identifier(), expectedCql);
            LOGGER.info("createTableAllOptional() - cqlComponent: {}, actualCql: {}", cqlComponent.identifier(), actualCql);

            assertThat(actualCql)
                    .as("Metadata CQL for '%s' should be as expected", cqlComponent.identifier())
                    .isEqualTo(expectedCql);
        }

    }

}
