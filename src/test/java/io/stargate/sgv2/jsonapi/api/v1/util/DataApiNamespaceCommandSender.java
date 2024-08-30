package io.stargate.sgv2.jsonapi.api.v1.util;

import static io.restassured.RestAssured.given;

import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.api.v1.NamespaceResource;

public class DataApiNamespaceCommandSender
    extends DataApiCommandSenderBase<DataApiNamespaceCommandSender> {
  protected DataApiNamespaceCommandSender(String namespace) {
    super(namespace);
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
            .post(NamespaceResource.BASE_PATH, namespace)
            .then()
            .statusCode(expectedHttpStatus);
    return new DataApiResponseValidator(response);
  }

  public DataApiResponseValidator postCreateTable(String tableDefAsJSON) {
    return postRaw("{ \"createTable\": %s }".formatted(tableDefAsJSON));
  }
}
