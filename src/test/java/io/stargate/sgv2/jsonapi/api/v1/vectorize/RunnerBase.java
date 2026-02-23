package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import static io.restassured.RestAssured.given;

public abstract class RunnerBase {

  protected abstract TestEnvironment integrationEnv();

}
