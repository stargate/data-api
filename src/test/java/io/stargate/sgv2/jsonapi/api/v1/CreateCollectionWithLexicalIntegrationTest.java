package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
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
@QuarkusTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateCollectionWithLexicalIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  @Nested
  @Order(1)
  class CreateLexicalHappyPath {
    @Test
    void createLexicalSimpleEnabledMinimal() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String collectionName = "coll_lexical_minimal" + RandomStringUtils.randomNumeric(16);
      String json = createRequestWithLexical(collectionName, "{\"enabled\": true}");

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection(collectionName);
    }

    @Test
    void createLexicalSimpleEnabledStandard() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

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

    // [data-api#2001]: Empty Analyzer object should be fine
    @Test
    void createLexicalSimpleEnabledEmptyObject() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String collectionName = "coll_lexical_emptyob" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                                {
                                  "enabled": true,
                                  "analyzer": { }
                                }
                          """);

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection(collectionName);
    }

    @Test
    void createLexicalSimpleEnabledCustom() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

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
    void createLexicalAdvancedCustom() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String collectionName = "coll_lexical_advanced_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                      {
                        "enabled": true,
                        "analyzer": {
                          "tokenizer" : {"name" : "standard"},
                          "filters": [
                            { "name": "lowercase" },
                            { "name": "stop" },
                            { "name": "porterstem" },
                            { "name": "asciifolding" }
                          ]
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

    @Test
    void createLexicalDisabledWithEmptyAnalyzerObject() {
      // Fine regardless of whether Lexical available for DB or not

      final String collectionName =
          "coll_lexical_disabled_empty" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(collectionName, "{\"enabled\": false, \"analyzer\": {}}");

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
    void failCreateLexicalWithDisabledAndAnalyzerString() {
      final String collectionName = "coll_lexical_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                            {
                              "enabled": false,
                              "analyzer": ""
                            }
                            """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "'lexical' is disabled, but 'lexical.analyzer' property was provided with an unexpected type: String"));
    }

    @Test
    void failCreateLexicalWithDisabledAndArrayInAnalyzer() {
      final String collectionName = "coll_lexical_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                            {
                              "enabled": false,
                              "analyzer": []
                            }
                            """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "'lexical' is disabled, but 'lexical.analyzer' property was provided with an unexpected type: Array."));
    }

    @Test
    void failCreateLexicalWithDisabledAndNumberInAnalyzer() {
      final String collectionName = "coll_lexical_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                              {
                                "enabled": false,
                                "analyzer": 1
                              }
                              """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "'lexical' is disabled, but 'lexical.analyzer' property was provided with an unexpected type: Number."));
    }

    @Test
    void failCreateLexicalWithDisabledAndAnalyzerObject() {
      final String collectionName = "coll_lexical_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                            {
                              "enabled": false,
                              "analyzer": {
                                "tokenizer": {
                                  "name": "whitespace"
                                }
                              }
                            }
                            """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "When 'lexical' is disabled, 'lexical.analyzer' must either be omitted or be JSON null, or"));
    }

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

      if (isLexicalAvailableForDB()) {
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

      if (isLexicalAvailableForDB()) {
        givenHeadersPostJsonThenOk(json)
            .body("$", responseIsError())
            .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
            // Not ideal: but Cassandra has pretty sub-optimal message for unknown pre-defined
            // analyzers
            .body(
                "errors[0].message",
                containsString(
                    "'analyzer' property of 'lexical' must be either JSON Object or String, is: Array"));
      } else {
        givenHeadersPostJsonThenOk(json)
            .body("$", responseIsError())
            .body("errors[0].errorCode", is("LEXICAL_NOT_AVAILABLE_FOR_DATABASE"));
      }
    }

    // [data-api#2011]
    @Test
    void failCreateLexicalMisspelledTokenizer() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String collectionName = "coll_lexical_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                      {
                        "enabled": true,
                        "analyzer": {
                          "tokeniser": {"name": "standard", "args": {}},
                          "filters": [
                              {"name": "lowercase"},
                              {"name": "stop"},
                              {"name": "porterstem"},
                              {"name": "asciifolding"}
                          ],
                          "extra": 123
                        }
                      }
                      """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "Invalid fields for 'lexical.analyzer'. Valid fields are: [charFilters, filters, tokenizer], found: [extra, tokeniser]"));
    }

    // [data-api#2011]
    @Test
    void failCreateLexicalNonObjectForTokenizer() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String collectionName = "coll_lexical_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                              {
                                "enabled": true,
                                "analyzer": {
                                  "tokenizer": false
                                }
                              }
                              """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "'tokenizer' property of 'lexical.analyzer' must be JSON Object, is: Boolean"));
    }

    // [data-api#2011]
    @Test
    void failCreateLexicalNonArrayForFilters() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String collectionName = "coll_lexical_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                              {
                                "enabled": true,
                                "analyzer": {
                                  "filters": { }
                                }
                              }
                              """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "'filters' property of 'lexical.analyzer' must be JSON Array, is: Object"));
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
