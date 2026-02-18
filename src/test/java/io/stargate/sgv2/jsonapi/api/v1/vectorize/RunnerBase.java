package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;

import java.util.Map;

import static io.restassured.RestAssured.given;

public abstract class RunnerBase {

  protected abstract IntegrationEnv  integrationEnv();

}
