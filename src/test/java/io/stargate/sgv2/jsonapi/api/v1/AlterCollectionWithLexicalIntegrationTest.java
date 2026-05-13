package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class AlterCollectionWithLexicalIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  @Nested
  @Order(1)
  class AlterCollectionEnableLexicalHappyPath {

    @Test
    void enableLexicalDefaultAnalyzerOnDisabledCollection() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      createCollectionWithLexicalDisabled(name);

      String json =
          """
          {
            "alterCollection": {
              "lexical": { "enabled": true }
            }
          }
          """;
      postToCollection(name, json)
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Sanity check: lexical insert/find should now work via $lexical sort.
      String insertOk =
          """
          {
            "insertOne": {
              "document": { "_id": "doc1", "$lexical": "hello world" }
            }
          }
          """;
      postToCollection(name, insertOk)
          .statusCode(200)
          .body("errors", is(org.hamcrest.Matchers.nullValue()));

      String find =
          """
          {
            "findOne": {
              "sort": { "$lexical": "hello" }
            }
          }
          """;
      postToCollection(name, find)
          .statusCode(200)
          .body("errors", is(org.hamcrest.Matchers.nullValue()))
          .body("data.document._id", is("doc1"));

      deleteCollection(name);
    }

    @Test
    void enableLexicalCustomAnalyzerOnDisabledCollection() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      createCollectionWithLexicalDisabled(name);

      String json =
          """
          {
            "alterCollection": {
              "lexical": {
                "enabled": true,
                "analyzer": { "tokenizer": { "name": "whitespace" } }
              }
            }
          }
          """;
      postToCollection(name, json)
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection(name);
    }

    // Locks in the surgical-replace contract of buildUpdatedComment: when alterCollection enables
    // lexical, all other previously-configured collection options (vector, indexing, defaultId,
    // rerank) must remain unchanged in the stored comment.
    @Test
    void preservesOtherOptionsAcrossAlter() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      String createBody =
              """
              {
                "createCollection": {
                  "name": "%s",
                  "options": {
                    "defaultId": { "type": "objectId" },
                    "vector": { "dimension": 5, "metric": "cosine" },
                    "indexing": { "deny": ["comment"] },
                    "lexical": { "enabled": false },
                    "rerank": { "enabled": false }
                  }
                }
              }
              """
              .formatted(name);
      given()
          .port(getTestPort())
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createBody)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Enable lexical via alterCollection.
      String alterBody =
          """
          {
            "alterCollection": {
              "lexical": { "enabled": true }
            }
          }
          """;
      postToCollection(name, alterBody)
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Verify via findCollections + explain that everything except lexical is unchanged,
      // and that lexical has flipped to enabled with the default analyzer.
      String expected =
              """
              {
                "name": "%s",
                "options": {
                  "defaultId": { "type": "objectId" },
                  "vector": { "dimension": 5, "metric": "cosine", "sourceModel": "other" },
                  "indexing": { "deny": ["comment"] },
                  "lexical": { "enabled": true, "analyzer": "standard" },
                  "rerank": { "enabled": false }
                }
              }
              """
              .formatted(name);
      given()
          .port(getTestPort())
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
              {
                "findCollections": {
                  "options": { "explain": true }
                }
              }
              """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body(
              "status.collections.find { it.name == '%s' }".formatted(name), jsonEquals(expected));

      deleteCollection(name);
    }

    @Test
    void enableLexicalAlreadyEnabledSameSettingsIsNoOp() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      // Create with lexical enabled, default analyzer.
      createCollectionWithLexical(name, "{ \"enabled\": true, \"analyzer\": \"standard\" }");

      String json =
          """
          {
            "alterCollection": {
              "lexical": { "enabled": true, "analyzer": "standard" }
            }
          }
          """;
      postToCollection(name, json)
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection(name);
    }
  }

  @Nested
  @Order(2)
  class AlterCollectionLexicalFail {

    @Test
    void failEnableLexicalDifferentAnalyzer() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      createCollectionWithLexical(name, "{ \"enabled\": true, \"analyzer\": \"standard\" }");

      String json =
          """
          {
            "alterCollection": {
              "lexical": {
                "enabled": true,
                "analyzer": { "tokenizer": { "name": "whitespace" } }
              }
            }
          }
          """;
      postToCollection(name, json)
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].errorCode",
              is(SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.name()))
          .body("errors[0].message", containsString("different analyzer configuration"));

      deleteCollection(name);
    }

    @Test
    void failDisableLexical() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      createCollectionWithLexical(name, "{ \"enabled\": true }");

      String json =
          """
          {
            "alterCollection": {
              "lexical": { "enabled": false }
            }
          }
          """;
      postToCollection(name, json)
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].errorCode",
              is(SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.name()))
          .body("errors[0].message", containsString("cannot disable lexical"));

      deleteCollection(name);
    }

    @Test
    void failUnknownRootField() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      createCollectionWithLexicalDisabled(name);

      String json =
          """
          {
            "alterCollection": {
              "lexical": { "enabled": true },
              "unknownField": 1
            }
          }
          """;
      // Jackson rejects the unknown root property; we just assert an error is returned.
      postToCollection(name, json).statusCode(200).body("$", responseIsError());

      deleteCollection(name);
    }

    @Test
    void failMissingLexical() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      createCollectionWithLexicalDisabled(name);

      String json =
          """
          {
            "alterCollection": { }
          }
          """;
      postToCollection(name, json)
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].errorCode",
              is(SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.name()))
          .body("errors[0].message", containsString("must specify 'lexical' field"));

      deleteCollection(name);
    }

    // Malformed `lexical` body must yield INVALID_ALTER_COLLECTION_OPTIONS (not the
    // createCollection variant). Mirrors the equivalent CreateCollectionWithLexicalIntegrationTest
    // failure cases, but with the alterCollection-specific error code.
    @Test
    void failMissingEnabledFlag() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      createCollectionWithLexicalDisabled(name);

      String json =
          """
          {
            "alterCollection": {
              "lexical": { }
            }
          }
          """;
      postToCollection(name, json)
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].errorCode",
              is(SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.name()))
          .body(
              "errors[0].message",
              containsString("'enabled' is required property for 'lexical' Object value"));

      deleteCollection(name);
    }

    @Test
    void failAnalyzerWrongJsonType() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      createCollectionWithLexicalDisabled(name);

      String json =
          """
          {
            "alterCollection": {
              "lexical": {
                "enabled": true,
                "analyzer": [1, 2, 3]
              }
            }
          }
          """;
      postToCollection(name, json)
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].errorCode",
              is(SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.name()))
          .body(
              "errors[0].message",
              containsString(
                  "'analyzer' property of 'lexical' must be either JSON Object or String, is: Array"));

      deleteCollection(name);
    }

    @Test
    void failAnalyzerMisspelledField() {
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      createCollectionWithLexicalDisabled(name);

      String json =
          """
          {
            "alterCollection": {
              "lexical": {
                "enabled": true,
                "analyzer": {
                  "tokeniser": { "name": "standard" }
                }
              }
            }
          }
          """;
      postToCollection(name, json)
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].errorCode",
              is(SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.name()))
          .body(
              "errors[0].message",
              containsString(
                  "Invalid field for 'lexical.analyzer'. Valid fields are: [charFilters, filters, tokenizer], found: [tokeniser]"));

      deleteCollection(name);
    }

    @Test
    void failEnableWhenLexicalNotAvailableForDB() {
      Assumptions.assumeFalse(isLexicalAvailableForDB());

      final String name = freshCollectionName();
      createCollectionWithLexicalDisabled(name);

      String json =
          """
          {
            "alterCollection": {
              "lexical": { "enabled": true }
            }
          }
          """;
      postToCollection(name, json)
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].errorCode",
              is(SchemaException.Code.LEXICAL_NOT_AVAILABLE_FOR_DATABASE.name()));

      deleteCollection(name);
    }
  }

  // -----------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------

  private static String freshCollectionName() {
    return "alter_lex_" + RandomStringUtils.insecure().nextAlphanumeric(12);
  }

  private void createCollectionWithLexicalDisabled(String collectionName) {
    createCollectionWithLexical(collectionName, "{ \"enabled\": false }");
  }

  private void createCollectionWithLexical(String collectionName, String lexicalDef) {
    String body =
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
    given()
        .port(getTestPort())
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(body)
        .when()
        .post(KeyspaceResource.BASE_PATH, keyspaceName)
        .then()
        .statusCode(200)
        .body("$", responseIsDDLSuccess())
        .body("status.ok", is(1));
  }

  private ValidatableResponse postToCollection(String collectionName, String json) {
    return given()
        .port(getTestPort())
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
        .then();
  }
}
