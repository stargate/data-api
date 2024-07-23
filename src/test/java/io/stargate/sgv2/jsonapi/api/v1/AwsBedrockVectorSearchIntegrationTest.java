package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(
    named = AwsBedrockVectorSearchIntegrationTest.BEDROCK_ACCESS_KEY_ID,
    matches = ".+")
@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class AwsBedrockVectorSearchIntegrationTest extends AbstractNamespaceIntegrationTestBase {
  static final String BEDROCK_ACCESS_KEY_ID = "BEDROCK_ACCESS_KEY_ID";
  static final String BEDROCK_SECRET_ID = "BEDROCK_SECRET_ID";

  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class BedrockTests {
    final Map<String, ?> headers =
        Map.of(
            HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
            getHeaders().get(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME),
            HttpConstants.EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME,
            System.getenv(BEDROCK_ACCESS_KEY_ID),
            HttpConstants.EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME,
            System.getenv(BEDROCK_SECRET_ID));

    @Test
    @Order(1)
    public void happyPathVectorSearch() {

      String json =
          """
                {
                    "createCollection": {
                        "name": "aws_bedrock_vectorize",
                        "options": {
                            "vector": {
                                "metric": "cosine",
                                "service": {
                                    "provider": "bedrock",
                                    "modelName": "amazon.titan-embed-text-v2:0",
                                    "parameters": {
                                        "region": "us-east-1"
                                    }
                                }
                            }
                        }
                    }
                }
                """;
      given()
          .headers(headers)
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }

    @Test
    @Order(2)
    public void insertVectorSearch() {
      String json =
          """
                {
                   "insertOne": {
                      "document": {
                          "_id": "1",
                          "name": "Coded Cleats",
                          "description": "ChatGPT integrated sneakers that talk to you",
                          "$vectorize": "ChatGPT integrated sneakers that talk to you"
                      }
                   }
                }
                """;

      given()
          .headers(headers)
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, "aws_bedrock_vectorize")
          .then()
          .statusCode(200)
          .body("status.insertedIds[0]", is("1"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
                {
                  "find": {
                    "filter" : {"_id" : "1"},
                    "projection": { "$vector": 1 }
                  }
                }
                """;

      given()
          .headers(headers)
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, "aws_bedrock_vectorize")
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.documents[0]._id", is("1"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vector", hasSize(1024));
    }

    @Test
    @Order(4)
    public void findOne() {
      String json =
          """
                        {
                          "findOne": {
                            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"},
                            "projection" : {"_id" : 1, "$vector" : 1, "$vectorize" : 1},
                            "options" : {"includeSimilarity" : true}
                          }
                        }
                        """;

      given()
          .headers(headers)
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, "aws_bedrock_vectorize")
          .then()
          .statusCode(200)
          .body("data.document._id", is("1"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.$vectorize", is(notNullValue()))
          .body("data.document.$similarity", greaterThan(0.0f))
          .body("errors", is(nullValue()));
    }
  }
}
