package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.Describable;
import io.stargate.sgv2.jsonapi.TestConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingCQL.collapseWhitespace;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class SuperShreddingBuilderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SuperShreddingBuilderTest.class);

    protected final TestConstants TEST_CONSTANTS = new TestConstants();

    // see constantIdentifiers
    private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("keyspace");
    private static final CqlIdentifier TABLE = CqlIdentifier.fromInternal("documents");

    protected static final String COMMENT = """
            {"collection":{"name":"documents","schema_version":2}}""";

    protected static final int VECTOR_LENGTH = 1024;
    protected static final String VECTOR_SIMILARITY_FUNCTION = "cosine";
    protected static final String VECTOR_SOURCE_MODEL = "OTHER";

    protected static final String LEXICAL_INDEX_ANALYZER = "standard";

    // NOTE: For validating the output of CQLBuilder against constant CQL we need
    // static keyspace & table names, other tests should use TestConstants.
    protected final boolean constantIdentifiers;

    // When creating CQL from Table or Index Metadata they do not add an IF NOT EXISTS
    // so when comparing the CQL from one of these we need to set
    // ifNotExists to false.
    // BUT when testing the ground truth with CqlBuilderTest or testing
    // SchmeaBiulder against CqlBUilder will normally want it enabled
    protected final boolean ifNotExists;

    protected SuperShreddingBuilderTest(){
        this(false, true);
    }

    protected SuperShreddingBuilderTest(boolean constantIdentifiers, boolean ifNotExists){
        this.constantIdentifiers = constantIdentifiers;
        this.ifNotExists = ifNotExists;
    }

    protected CqlIdentifier keyspace(){
        return constantIdentifiers ? KEYSPACE : TEST_CONSTANTS.COLLECTION_IDENTIFIER.keyspace();
    }

    protected CqlIdentifier table(){
        return constantIdentifiers ? TABLE : TEST_CONSTANTS.COLLECTION_IDENTIFIER.table();
    }

    protected <U, T extends SuperShreddingBuilder<U, T>> T configDefault(T builder) {
        return builder
                .withKeyspace(keyspace())
                .withCollection(table())
                .withIfNotExists(ifNotExists);
    }

    protected <U, T extends SuperShreddingBuilder<U, T>> T configAllOptional(T builder) {
        return configDefault(builder)
                .withComment(COMMENT)
                .withVector(VECTOR_LENGTH, VECTOR_SIMILARITY_FUNCTION, VECTOR_SOURCE_MODEL)
                .withLexical(LEXICAL_INDEX_ANALYZER);
    }

    protected <U, T extends SuperShreddingBuilder<U, T>> T configNoOptional(T builder) {
        return configDefault(builder)
                .withComment(COMMENT);
    }

    protected <U, T extends SuperShreddingBuilder<U, T>> T configVectorOnly(T builder) {
        return configDefault(builder)
                .withComment(COMMENT)
                .withVector(VECTOR_LENGTH, VECTOR_SIMILARITY_FUNCTION, VECTOR_SOURCE_MODEL);
    }

    protected <U, T extends SuperShreddingBuilder<U, T>> T configLexicalOnly(T builder) {
        return configDefault(builder)
                .withComment(COMMENT)
                .withLexical(LEXICAL_INDEX_ANALYZER);
    }


    protected static List<SuperShreddingBuilder.SuperShreddingComponent<?>> upcastString(List<SuperShreddingBuilder.SuperShreddingComponent<String>> components){
        return new ArrayList<>(components);
    }

    protected static List<SuperShreddingBuilder.SuperShreddingComponent<?>> upcastDesc(List<SuperShreddingBuilder.SuperShreddingComponent<Describable>> components){
        return new ArrayList<>(components);
    }


    protected void assertComponents(String testName,
                                         List<SuperShreddingBuilder.SuperShreddingComponent<?>> expectedComponents,
                                         List<SuperShreddingBuilder.SuperShreddingComponent<?>> actualComponents){

        Objects.requireNonNull(expectedComponents, "expectedComponents must be null");
        Objects.requireNonNull(actualComponents, "actualComponents must be null");

        assertThat(actualComponents)
                .as("%s - Components same size as expected", testName)
                .hasSize(expectedComponents.size());

        for (var expected : expectedComponents) {

            var actual = actualComponents.stream()
                    .filter(component -> component.identifier().equals(expected.identifier()))
                    .findFirst()
                    .orElse(null);
            assertThat(actual)
                .as("%s - Expected Component '%s' not found in actual",testName, expected.identifier())
                .isNotNull();

            assertThat(actual.type())
                    .as("%s - Actual Component with name '%s' should be of type '%s'", testName, expected.identifier(), expected.type())
                    .isEqualTo(expected.type());


            var expectedCQL = collapseWhitespace(expected.asCql());
            var actualCql = collapseWhitespace(actual.asCql());

            if (LOGGER.isInfoEnabled()){
                // extra spaces to line up for easier reading
                LOGGER.info("assertTableCql() - testName: {}, expectedCOL: {}", testName, expectedCQL);
                LOGGER.info("assertTableCql() - testName: {},   actualCQL: {}", testName, actualCql);
            }

            assertThat(actualCql)
                    .as("%s - Actual CQL for component '%s' should match expected", testName, expected.identifier())
                    .isEqualTo(expectedCQL);

        }

        Set<CqlIdentifier> expectedIdentifiers = expectedComponents.stream().
                map(SuperShreddingBuilder.SuperShreddingComponent::identifier)
                .collect(Collectors.toSet());

        var unexpectedComponents = actualComponents.stream()
                .filter(component -> !expectedIdentifiers.contains(component.identifier()))
                .toList();

        assertThat(unexpectedComponents)
                .as("%s - No unexpected components found", testName)
                .isEmpty();
    }
}
