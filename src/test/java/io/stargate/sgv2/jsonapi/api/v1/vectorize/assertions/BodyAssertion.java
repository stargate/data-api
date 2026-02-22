package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.APIResponse;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCase;
import org.hamcrest.Matcher;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWriteSuccess;
import static org.hamcrest.Matchers.*;

public record BodyAssertion(
    String bodyPath,
    Matcher<?> matcher
) implements TestAssertion{

  public void run(APIResponse apiResponse) {

    apiResponse.validatableResponse().body(bodyPath(), matcher());
  }
}