package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.model.command.CommandTarget;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.api.v1.GeneralResource;
import io.stargate.sgv2.jsonapi.api.v1.KeyspaceResource;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.AssertionFactory;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.ITAssertion;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWriteSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.getCassandraPassword;
import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.getCassandraUsername;
import static org.hamcrest.Matchers.*;

public class IntegrationTestRunner extends RunnerBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationEnv.class);


  // keyspace automatically created in this test
  protected static final String keyspaceName =
      "ks" + RandomStringUtils.insecure().nextAlphanumeric(16);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ITCollection itCollection;
  private final IntegrationTest test;
  private final IntegrationEnv env;

  public IntegrationTestRunner(
      ITCollection itCollection, IntegrationTest test, IntegrationEnv env) {
    this.itCollection = itCollection;
    this.test = test;
    this.env = env;
  }

  @Override
  protected IntegrationEnv integrationEnv() {
    return env;
  }

  public void run() {

    createKeyspace(keyspaceName);

    for (TestRequest setupRequest : test.setup()) {

      var requestWithEnv = setupRequest.withEnvironment(env);
      var requestSpec = setupRequest(requestWithEnv);
      var response = executeRequest(setupRequest, requestSpec);
      assertSetup(setupRequest, requestWithEnv, response);
    }

    for (TestItem testItem : test.tests()) {

      var requestWithEnv = testItem.request().withEnvironment(env);
      var requestSpec = setupRequest(requestWithEnv);
      var response = executeRequest(testItem.request(), requestSpec);

      testAssertions(testItem, response);
    }
  }

  protected void createKeyspace(String keyspace) {
    String json =
        """
            {
              "createKeyspace": {
                "name": "%s"
              }
            }
            """
            .formatted(keyspace);

    jsonRequest()
        .body(json)
        .when()
        .post(GeneralResource.BASE_PATH)
        .then()
        .statusCode(200)
        .body("$", responseIsDDLSuccess())
        .body("status.ok", is(1));
  }

  private RequestSpecification setupRequest(ObjectNode request) {

    String requestString;
    try {
      requestString = OBJECT_MAPPER.writeValueAsString(request);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return jsonRequest()
        .body(requestString).when();
  }

  private ValidatableResponse executeRequest(
      TestRequest testRequest, RequestSpecification requestSpec) {

    if (testRequest.commandName().getTargets().contains(CommandTarget.COLLECTION) || testRequest.commandName().getTargets().contains(CommandTarget.TABLE)){
      return requestSpec
          .post(CollectionResource.BASE_PATH, keyspaceName, env.vars().get("COLLECTION_NAME"))
          .then()
          .log().all();
    }

    if (testRequest.commandName().getTargets().contains(CommandTarget.KEYSPACE) ){
      return requestSpec.post(KeyspaceResource.BASE_PATH, keyspaceName).then().log().all();
    }
    throw new IllegalArgumentException("Do not know how to execute command: " + testRequest.commandName());

  }

  private void assertSetup(
      TestRequest testRequest, ObjectNode requestWithEnv, ValidatableResponse response) {
    response.statusCode(200);

    switch (testRequest.commandName()) {
      case INSERT_ONE, INSERT_MANY -> {
        response
            .body("$", responseIsWriteSuccess())
            .body("status.insertedIds[0]", not(emptyString()));
      }
      case DELETE_COLLECTION, CREATE_COLLECTION -> {
        response
            .body("$", responseIsDDLSuccess())
            .body("status.ok", is(1));
      }
    }
  }

  private void testAssertions (TestItem testItem, ValidatableResponse response) {

    for (Map.Entry<String, JsonNode> entry : testItem.asserts().properties()){
      var args = entry.getValue();     // null / 3 / {...}

      var assertFactory = findAssertionFactory(entry.getKey());
      ITAssertion itAssertion;
      try {
        itAssertion = (ITAssertion)assertFactory.invoke(null, args);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }

      LOGGER.info("XXX Running test assertion key={}, value={}", entry.getKey(), entry.getValue().toString());
      response.body(itAssertion.bodyPath(), itAssertion.matcher());
    }
  }

  private Method findAssertionFactory(String key){
    // "response.isFindSuccess"

    int dot = key.indexOf('.');
    String typeName = key.substring(0, dot);
    String funcName   = key.substring(dot + 1);

    String qualifiedTypeName =
        "io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions."
            + Character.toUpperCase(typeName.charAt(0))
            + typeName.substring(1).toLowerCase();

    try {
      Class<?> cls = Class.forName(qualifiedTypeName);

      var factoryMethod = Arrays.stream(cls.getMethods())
              .filter(m -> m.getName().equalsIgnoreCase(funcName))
              .filter(m -> Modifier.isStatic(m.getModifiers()))
              .findFirst()
              .orElseThrow();


      return factoryMethod;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Invalid assertion: " + key, e);
    }
  }
}
