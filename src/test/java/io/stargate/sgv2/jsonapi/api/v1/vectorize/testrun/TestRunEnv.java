package io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSuiteSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookupFactory;
import org.junit.jupiter.api.DynamicContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRunEnv {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestRunEnv.class);
  private static final Pattern PATTERN_NOT_WORD_CHARS = Pattern.compile("\\W+");

  private static final Set<String> SCHEMA_IDENTIFIER = Set.of("KEYSPACE_NAME", "COLLECTION_NAME");

  private final Map<String, String> vars = new HashMap<>();

  public TestRunEnv() {
    this(new HashMap<>());
  }

  public TestRunEnv(Map<String, String> vars) {
    this.vars.putAll(vars);
  }

  public DynamicContainer testNode(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, TestSuiteSpec testSuite) {

    var d = description();
    uriBuilder.addSegment(TestUri.Segment.ENV, d);
    var desc = "TestEnv: %s ".formatted(d);

    var testExecutionCondition = new TestExecutionCondition.Default(desc);
    var envNodes = testSuite.testNodesForEnvironment(testNodeFactory, uriBuilder.clone(), this, testExecutionCondition);

    return testNodeFactory.testPlanContainer(
        desc,
        uriBuilder.build().uri(),
            testNodeFactory.addLifecycle(uriBuilder.clone(), testSuite, this, envNodes));
  }

  private String description() {
    return vars.entrySet().stream()
        .filter(
            entry -> {
              var name = entry.getKey().toUpperCase();
              if (SCHEMA_IDENTIFIER.contains(name)) {
                return false; // schema names not usually interesting
              }
              if (name.contains("KEY") || name.contains("API") || name.contains("TOKEN")) {
                return false; // assume security
              }
              return true;
            })
        .sorted(Map.Entry.comparingByKey())
        .toList()
        .toString();
  }

  private TestRunEnv(TestRunEnv other) {
    this.vars.putAll(other.vars);
  }

  public TestRunEnv clone() {
    return new TestRunEnv(this);
  }

  public TestRunEnv put(TestRunEnv other) {
    this.vars.putAll(other.vars);
    return this;
  }

  public void put(String key, String value) {
    this.vars.put(key, value);
  }

  public String requiredValue(String name) {
    if (vars.containsKey(name)) {
      return get(name);
    }
    throw new RuntimeException(
        String.format(
            "Required env var not found name:%s, defined: %s",
            name, String.join(", ", vars.keySet())));
  }

  public StringSubstitutor substitutor() {

    return new StringSubstitutor(StringLookupFactory.INSTANCE.functionStringLookup(this::get))
        .setEnableUndefinedVariableException(true);
  }

  public String get(String name) {

    var value = vars.get(name);
    if (value == null) {
      return "";
    }

    var substituted = substitutor().replace(value);
    var cleaned =
        SCHEMA_IDENTIFIER.contains(name) ? toSafeSchemaIdentifier(substituted) : substituted;

    return cleaned;
  }

  public static String toSafeSchemaIdentifier(String name) {

    var newValue = PATTERN_NOT_WORD_CHARS.matcher(name).replaceAll("_");
    if (newValue.length() > 48) {
      return newValue.substring(0, 47);
      //      throw new RuntimeException("Schema Identifier longer than 48 characters
      // orginalName=%s, afterNormalisation==%s".formatted(name,newValue));
    }
    return newValue;
  }

  @Override
  public String toString() {
    return vars.toString();
  }
}
