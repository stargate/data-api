package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.ITAssertion;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;

import static io.restassured.RestAssured.given;

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

    for (TestCase testItem : test.tests()) {

    }

    for (TestRequest setupRequest : test.cleanup()) {
      target.apiRequest(setupRequest, env).executeWithSuccess();
    }
  }

  private TestResult runTest(TestCase testItem, IntegrationEnv env) {

    AssertionError textException;
    ValidatableResponse response;
    try {
      response = target.apiRequest(testItem.request(), env).execute();
      testAssertions(testItem, response);
    }
    catch (AssertionError ae){
      textException = ae;
    }

    response.extract().
  }
  private void testAssertions (TestCase testItem, ValidatableResponse response) {

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
