package io.stargate.sgv2.jsonapi.exception;

import java.util.Collections;
import java.util.Map;

/**
 * Raised by the Exception code when a template used to build an error message has a variable in it
 * that is not resolved by the values passed when the exception is created.
 *
 * <p>E.G. if the template is "too many ${foo}" and the values passed are "bar" and "baz" then this
 * exception is raised.
 *
 * <p>This <b>SHOULD NOT</b> happen in production code, exceptions should be triggered in testing
 * and this exception is raised if the template and the code do not match. Note that it is OK for
 * the code to provide more variables than the template uses, but not the other way around.
 */
public class UnresolvedErrorTemplateVariable extends RuntimeException {

  public final ErrorTemplate<?> errorTemplate;
  public final Map<String, String> variables;

  /**
   * Create a new instance.
   *
   * <p>This is the message the Apache libs will generate: <code>
   *    String.format("Cannot resolve variable '%s' (enableSubstitutionInVariables=%s).",
   *                                         varName, substitutionInVariablesEnabled))
   *  </code>
   *
   * @param errorTemplate The template we were processing when the error occurred.
   * @param message The Apache {@link org.apache.commons.text.StringSubstitutor} raises a {@link
   *     IllegalArgumentException} with the message above, pass this as the message to this
   *     exception.
   */
  public UnresolvedErrorTemplateVariable(
      ErrorTemplate<?> errorTemplate, Map<String, String> variables, String message) {
    super(formatMessage(errorTemplate, variables, message));
    this.errorTemplate = errorTemplate;
    this.variables = Collections.unmodifiableMap(variables);
  }

  private static String formatMessage(
      ErrorTemplate<?> errorTemplate, Map<String, String> variables, String message) {
    return String.format(
        "Unresolved variable in error template for family: %s, scope: %s, code: %s, keys: %s error: %s",
        errorTemplate.family(),
        errorTemplate.scope(),
        errorTemplate.code(),
        variables.keySet(),
        message);
  }
}
