package io.stargate.sgv2.jsonapi.api.v1.util;

import static io.restassured.RestAssured.given;

import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;

public class DataApiTableCommandSender extends DataApiCommandSenderBase<DataApiTableCommandSender> {
  private final String tableName;

  protected DataApiTableCommandSender(String namespace, String tableName) {
    super(namespace);
    this.tableName = tableName;
  }

  /**
   * "Untyped" method for sending a POST command to the Data API: caller is responsible for
   * formatting the JSON body correctly.
   *
   * @param jsonBody JSON body to POST
   * @return Response validator for further assertions
   */
  public DataApiResponseValidator postRaw(String jsonBody) {
    ValidatableResponse response =
        given()
            .port(getTestPort())
            .headers(getHeaders())
            .contentType(ContentType.JSON)
            .body(jsonBody)
            .when()
            .post(CollectionResource.BASE_PATH, namespace, tableName)
            .then()
            .statusCode(expectedHttpStatus);
    return new DataApiResponseValidator(response);
  }

  /**
   * Partially typed method for sending a POST command to the Data API: caller is responsible for
   * formatting the clause to include as (JSON Object) argument of "finOne" command.
   *
   * @param findOneClause JSON clause to include in the "findOne" command: minimally empty JSON
   *     Object ({@code { } })
   * @return Response validator for further assertions
   */
  public DataApiResponseValidator postFindOne(String findOneClause) {
    return postRaw("{ \"findOne\": %s }".formatted(findOneClause));
  }

  public DataApiResponseValidator postInsertOne(String docAsJSON) {
    return postRaw(
            """
            {
              "insertOne": {
                "document": %s
              }
            }
            """
            .formatted(docAsJSON));
  }
}
