package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateCollectionWithLexicalIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  @Nested
  @Order(1)
  class CreateLexicalHappyPath {
    @Test
    void createLexicalSimpleEnabledMinimal() {
      Assumptions.assumeTrue(isLexicalAvailable());

      final String collectionName = "coll_lexical_minimal" + RandomStringUtils.randomNumeric(16);
      String json = createRequestWithLexical(collectionName, "{\"enabled\": true}");

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection(collectionName);
    }

    @Test
    void createLexicalSimpleEnabledStandard() {
      Assumptions.assumeTrue(isLexicalAvailable());

      final String collectionName = "coll_lexical_simple" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                        {
                          "enabled": true,
                          "analyzer": "standard"
                        }
                  """);

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection(collectionName);
    }

    @Test
    void createLexicalSimpleEnabledCustom() {
      Assumptions.assumeTrue(isLexicalAvailable());

      final String collectionName = "coll_lexical_cust_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                                {
                                  "enabled": true,
                                  "analyzer": {
                                    "tokenizer": {
                                       "name": "whitespace"
                                    }
                                  }
                                }
                          """);

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection(collectionName);
    }

    @Test
    void createLexicalSimpleDisabled() {
      // Fine regardless of whether Lexical available for DB or not

      final String collectionName = "coll_lexical_disabled" + RandomStringUtils.randomNumeric(16);
      String json = createRequestWithLexical(collectionName, "{\"enabled\": false}");

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection(collectionName);
    }
  }

  @Nested
  @Order(2)
  class CreateLexicalFail {
    @Test
    void failCreateLexicalMissingEnabled() {
      final String collectionName = "coll_lexical_" + RandomStringUtils.randomNumeric(16);
      String json = createRequestWithLexical(collectionName, "{ }");

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "The provided options are invalid: 'enabled' is required property for 'lexical'"));
    }

    @Test
    void failCreateLexicalUnknownAnalyzer() {
      final String collectionName = "coll_lexical_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                                {
                                  "enabled": true,
                                  "analyzer": "unknown"
                                }
                          """);

      if (isLexicalAvailable()) {
        givenHeadersPostJsonThenOk(json)
            .body("$", responseIsError())
            .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
            // Not ideal: but Cassandra has pretty sub-optimal message for unknown pre-defined
            // analyzers
            .body("errors[0].message", containsString("Invalid analyzer config"))
            .body("errors[0].message", containsString("token 'unknown'"));
      } else {
        givenHeadersPostJsonThenOk(json)
            .body("$", responseIsError())
            .body("errors[0].errorCode", is("LEXICAL_NOT_AVAILABLE_FOR_DATABASE"));
      }
    }

    @Test
    void failCreateLexicalWrongJsonType() {
      final String collectionName = "coll_lexical_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                                        {
                                          "enabled": true,
                                          "analyzer": [ 1, 2, 3 ]
                                        }
                                  """);

      if (isLexicalAvailable()) {
        givenHeadersPostJsonThenOk(json)
            .body("$", responseIsError())
            .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
            // Not ideal: but Cassandra has pretty sub-optimal message for unknown pre-defined
            // analyzers
            .body(
                "errors[0].message",
                containsString(
                    "'analyzer' property of 'lexical' must be either String or JSON Object, is: ARRAY"));
      } else {
        givenHeadersPostJsonThenOk(json)
            .body("$", responseIsError())
            .body("errors[0].errorCode", is("LEXICAL_NOT_AVAILABLE_FOR_DATABASE"));
      }
    }
  }

  private String createRequestWithLexical(String collectionName, String lexicalDef) {
    return
        """
                  {
                    "createCollection": {
                      "name": "%s",
                      "options": {
                        "lexical": %s
                      }
                    }
                  }
                  """
        .formatted(collectionName, lexicalDef);
  }
}
