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
  private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTestRunner.class);


  // keyspace automatically created in this test
  protected static final String keyspaceName =
      "ks" + RandomStringUtils.insecure().nextAlphanumeric(16);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ITCollection itCollection;
  private final IntegrationTarget target;
  private final IntegrationTest test;
  private final IntegrationEnv env;

  public IntegrationTestRunner(
      ITCollection itCollection, IntegrationTarget target,  IntegrationTest test, IntegrationEnv env) {
    this.itCollection = itCollection;
    this.target = target;
    this.test = test;
    this.env = env;
  }

  @Override
  protected IntegrationEnv integrationEnv() {
    return env;
  }

  public void run() {

    LOGGER.info("Starting Integration Test with env={}", env);
    for (TestRequest setupRequest : test.setup()) {
      target.apiRequest(setupRequest, env).executeWithSuccess();
    }

    for (TestItem testItem : test.tests()) {

      var resp = target.apiRequest(testItem.request(), env).execute();
      testAssertions(testItem, resp);
    }

    for (TestRequest setupRequest : test.cleanup()) {
      target.apiRequest(setupRequest, env).executeWithSuccess();
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
