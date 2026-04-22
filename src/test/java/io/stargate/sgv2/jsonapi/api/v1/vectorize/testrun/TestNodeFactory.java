package io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestPlan;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.Job;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSpecMeta;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSuiteSpec;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.WorkflowSpec;
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
import java.util.stream.Stream;

/**
 * A container we can pass around when building the TestNodes that has the test plan
 * we are working with, and has a counter for the number of nodes we created.
 *
 * <p>
 * So we can output them for the test reporter to report progress.
 * </p>
 */
public class TestNodeFactory {

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
        return DynamicContainer.dynamicContainer(displayName, testSourceUri, dynamicNodes.stream());
    }

    public DynamicTest testPlanTest(String description, URI uri, Executable executable){

        totalNodeCount++;
        return DynamicTest.dynamicTest(description, uri, executable);
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

//    private  Optional<DynamicContainer> containerIfPresent(
//            TestUri.Builder uriBuilder,
//            String namePrefix,
//            TestSpecMeta meta,
//            Optional<DynamicNode> dynamicNode) {
//
//        return dynamicNode.map(
//                node ->
//                        testPlanContainer(
//                                namePrefix + ": " + meta.name(), uriBuilder.build().uri(), List.of(node)));
//    }
//
//    private  Stream<DynamicNode> streamIfPresent(Optional<DynamicContainer> container) {
//        return container.stream().flatMap(Stream::of);
//    }

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
}
