package io.stargate.sgv2.jsonapi.testbench.testspec;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.testbench.assertions.AssertionFactory;

import java.util.Map;
import java.util.Optional;

/**
 * Spec defintions about re-usable assertions that are defined in JSON. See {@link AssertionFactory}.
 * <p>
 * Example, showing the defintion for "isSuccess" assertion and how it is defined on a per command basis.
 * <pre>
 * {
 *   "meta": {
 *     "name": "assertions-templates",
 *     "kind": "assertion_template"
 *   },
 *   "templates": {
 *     "isSuccess": {
 *       "createCollection": {
 *         "http.success": null,
 *         "response.isDDLSuccess": null
 *       },
 *       "find": {
 *         "http.success": null,
 *         "response.isFindSuccess": null
 *       },
 * </pre>
 *
 * </p>
 * @param meta Metadata for the spec.
 * @param templates JSON Object that is a map of assertion name to a map of implementations per API command,
 *                  see example
 */
public record AssertionTemplateSpec(TestSpecMeta meta, Map<String, JsonNode> templates)
    implements TestSpec {

  public Optional<JsonNode> templateFor(String assertionName) {
    return Optional.ofNullable(templates.get(assertionName));
  }
}
