package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import io.stargate.sgv2.jsonapi.TestConstants;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.function.Function;

import static io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingCQL.collapseWhitespace;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testing that when we build TableMetadata for super shredding table, it matches the expected CQL statement
 * from
 */
public class SuperShreddingMetadataBuilderTest  extends SuperShreddingBuilderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SuperShreddingMetadataBuilderTest.class);


    private final TestConstants TEST_CONSTANTS = new TestConstants();

    public SuperShreddingMetadataBuilderTest(){
        super(false, false);
        // ^^ ok to use dynamic schema names, but need to exclude ifNotexists because
        // cql from TableMetadata etc does not add it.
    }


    @Test
    public void createTableAllOptional() {

        var expectedCqlBuilder = configAllOptional(SuperShreddingCQLBuilder.cql());
        var actualMetadataBuilder = configAllOptional(SuperShreddingMetadataBuilder.metadata());

        assertComponents("createTableAllOptional()", upcastString(expectedCqlBuilder.build()), upcastDesc(actualMetadataBuilder.build()));
    }


    @Test
    public void createTableNoOptional(){

        var expectedCqlBuilder = configNoOptional(SuperShreddingCQLBuilder.cql());
        var actualMetadataBuilder = configNoOptional(SuperShreddingMetadataBuilder.metadata());

        assertComponents("createTableNoOptional()", upcastString(expectedCqlBuilder.build()), upcastDesc(actualMetadataBuilder.build()));
    }

    @Test
    public void createTableVectorOnly() {

        var expectedCqlBuilder = configVectorOnly(SuperShreddingCQLBuilder.cql());
        var actualMetadataBuilder = configVectorOnly(SuperShreddingMetadataBuilder.metadata());

        assertComponents("createTableVectorOnly()", upcastString(expectedCqlBuilder.build()), upcastDesc(actualMetadataBuilder.build()));
    }


    @Test
    public void createTableLexicalOnly() {

        var expectedCqlBuilder = configLexicalOnly(SuperShreddingCQLBuilder.cql());
        var actualMetadataBuilder = configLexicalOnly(SuperShreddingMetadataBuilder.metadata());

        assertComponents("createTableLexicalOnly()", upcastString(expectedCqlBuilder.build()), upcastDesc(actualMetadataBuilder.build()));
    }
}
