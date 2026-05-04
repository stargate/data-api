package io.stargate.sgv2.jsonapi.testbench.testrun;

import io.stargate.sgv2.jsonapi.testbench.TestPlan;
import io.stargate.sgv2.jsonapi.testbench.testspec.Job;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestSpecMeta;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestSuiteSpec;
import io.stargate.sgv2.jsonapi.testbench.testspec.WorkflowSpec;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.function.Executable;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A container we can pass around when building the TestNodes that has the test plan
 * we are working with, and has a counter for the number of nodes we created.
 *
 * <p>
 * So we can output them for the test reporter to report progress.
 * </p>
 */
public class TestNodeFactory {

    private final NodeCode nodeCode = new NodeCode();
    private final TestPlan testPlan;
    private int totalNodeCount = 0;

    public TestNodeFactory(TestPlan testPlan) {
        this.testPlan = testPlan;
    }

    public TestPlan testPlan() {
        return testPlan;
    }

    public int testNodeCount(){
        return totalNodeCount;
    }

    /**
     * NOTE: This is forcing the use of a List, so we greedily create all the test nodes, so we can count
     * how many their are, so we can show progress.
     * @param displayName
     * @param testSourceUri
     * @param dynamicNodes
     * @return
     */
    public DynamicContainer testPlanContainer(String displayName, URI testSourceUri,
                                      List<? extends DynamicNode> dynamicNodes){
        totalNodeCount++;
        return DynamicContainer.dynamicContainer(appendNodeCode(displayName), testSourceUri, dynamicNodes.stream());
    }

    public DynamicTest testPlanTest(String description, URI uri, Executable executable){

        totalNodeCount++;
        return DynamicTest.dynamicTest(appendNodeCode(description), uri, executable);
    }

    private String appendNodeCode(String displayName){
        return "%s (node:%s)".formatted(displayName, nodeCode.next());
    }

    public List<? extends DynamicNode> addLifecycle(
            TestUri.Builder uriBuilder,
            WorkflowSpec workflow,
            List<? extends DynamicNode> dynamicNodes) {

        var beforeUriBuilder =
                uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "before-workflow");
        var afterUriBuilder =
                uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "after-workflow");

        var nodes = new ArrayList<DynamicNode>();
        nodes.addAll(lifecycleNodes(
                        beforeUriBuilder,
                        "Before Workflow",
                        workflow.meta(),
                        () -> testPlan.target().beforeWorkflow(this, beforeUriBuilder, workflow)));
        nodes.addAll(dynamicNodes);
        nodes.addAll(lifecycleNodes(
                                afterUriBuilder,
                                "After Workflow",
                                workflow.meta(),
                                () -> testPlan.target().afterWorkflow(this, afterUriBuilder, workflow)));
        return nodes;
    }

    public List<? extends DynamicNode> addLifecycle(
            TestUri.Builder uriBuilder, Job job, List<? extends DynamicNode> dynamicNodes) {

        var beforeUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "before-job");
        var afterUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "after-job");

        var nodes = new ArrayList<DynamicNode>();
        nodes.addAll(lifecycleNodes(
                        beforeUriBuilder,
                        "Before Job",
                        job.meta(),
                        () -> testPlan.target().beforeJob(this, beforeUriBuilder, job)));
        nodes.addAll(dynamicNodes);
        nodes.addAll(lifecycleNodes(
                                afterUriBuilder,
                                "After Job",
                                job.meta(),
                                () -> testPlan.target().afterJob(this, afterUriBuilder, job)));
        return nodes;
    }

    public List<? extends DynamicNode> addLifecycle(
            TestUri.Builder uriBuilder,
            TestSuiteSpec testSuite,
            TestRunEnv environment,
            List<? extends DynamicNode> dynamicNodes) {

        var beforeUriBuilder =
                uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "before-test-suite");
        var afterUriBuilder =
                uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "after-test-suite");

        var nodes = new ArrayList<DynamicNode>();
        nodes.addAll(lifecycleNodes(
                        beforeUriBuilder,
                        "Before TestSuite",
                        testSuite.meta(),
                        () -> testPlan.target().beforeTestSuite(this, beforeUriBuilder, testSuite, environment)));
        nodes.addAll(dynamicNodes);
        nodes.addAll(lifecycleNodes(
                                afterUriBuilder,
                                "After TestSuite",
                                testSuite.meta(),
                                () -> testPlan.target().afterTestSuite(this, afterUriBuilder, testSuite, environment)));
        return nodes;
    }

    private  List<DynamicNode> lifecycleNodes(
            TestUri.Builder uriBuilder,
            String namePrefix,
            TestSpecMeta meta,
            Supplier<Optional<DynamicNode>> nodeSupplier) {

        var targetDynamicNode = nodeSupplier.get();

        if (targetDynamicNode.isEmpty()) {
            return Collections.emptyList();
        }

        var lifecycleContainer = testPlanContainer(
                namePrefix + ": " + meta.name(), uriBuilder.build().uri(), List.of(targetDynamicNode.get()));
        return List.of(lifecycleContainer);
    }

    public static class NodeCode {

        private static final char[] ALPHABET =
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
        private static final int BASE = ALPHABET.length; // 62

        // 3 characters of base 62 coding above gives 62^3 = 238,328
        private static final int LENGTH = 3;

        private int counter = 0;

        public String next() {
            int n = counter++;
            char[] code = new char[LENGTH];
            for (int i = LENGTH - 1; i >= 0; i--) {
                code[i] = ALPHABET[n % BASE];
                n /= BASE;
            }
            return new String(code);
        }
    }
}
