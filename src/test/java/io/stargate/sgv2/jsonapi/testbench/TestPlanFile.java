package io.stargate.sgv2.jsonapi.testbench;

import io.stargate.sgv2.jsonapi.testbench.testspec.TargetConfiguration;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Structure of a test plane file stored in a jar resource or external file. These are used with the
 * {@link TestBenchByTestPlan} runner to control the target and workflows to run at execution time via a config file.
 * <p>
 * For example <code>testbench/testplans/test-plan-astra-vectorize.yaml</code>, testing against and Astra DB:
 *
 * <pre>
 * name: Test Plan - Astra - Vectorize Workflows
 * customTarget:
 *   name: ${TARGET_NAME}
 *   backend: astra
 *   connection:
 *     domain: ${ENDPOINT}
 *     port: 443
 *     basePath: /api/json/v1
 * workflows:
 *   - vectorize-header-workflow
 *   - vectorize-shared-workflow
 * ignoreDisabled: true
 * </pre>
 * </p>
 *<p>
 * The {@link TestPlan#fromFile(Path)} will run Apache command style substituions using {@link System#getenv(String)}
 * as the source for replacements. This allows some sensitive information to be put into the env rather than a
 * file, and for the runner to make multiple calls with different env vars.
 * </p>
 * @param name Nice human name for the test plan, only used in this file
 * @param targetName Name of the target, such as the db name, used in logging etc.
 * @param customTarget Defines a {@link TargetConfiguration} of how to connect (e.g. astra or cassandra backend )
 *                     and the connection information.
 * @param workflows List of the workflows to run, leave empty or null to run all workflows in the system.
 * @param ignoreDisabled If true, work flow jobs marked as "disabled" will be executed. Default is false.
 */
public record TestPlanFile(
        String name,
        String targetName,
        TargetConfiguration customTarget,
        List<String> workflows,
        Boolean ignoreDisabled) {
}
