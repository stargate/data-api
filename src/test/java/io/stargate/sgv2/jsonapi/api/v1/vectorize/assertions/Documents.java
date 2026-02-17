package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;

import static org.hamcrest.Matchers.hasSize;

public class Documents {

  public static ITAssertion count(JsonNode args) {
    var expectedCount = args.asInt();
    return new ITAssertion("data.documents", hasSize(expectedCount));
  }
}
