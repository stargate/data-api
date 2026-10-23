package io.stargate.sgv2.jsonapi.testresource;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.TestResourceScope;
import io.quarkus.test.common.WithTestResource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.ClassDescriptor;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.ClassOrdererContext;

/**
 * Orders integration test classes so that Quarkus restarts the application and its test resources
 * as few times as possible.
 *
 * <p>Quarkus restarts when the next test class has a different set of test resources, or when
 * either class has a resource restricted to it ({@code restrictToAnnotatedClass = true}). Its
 * default orderer sorts integration test classes by name only, so a class with its own resources
 * usually costs two restarts: one into the class and one back to the shared resources. This orderer
 * runs the classes that only use {@link DseTestResource} first, then the other classes grouped by
 * their test resources, each group by class name.
 *
 * <p>Set as {@code quarkus.test.class-orderer} for failsafe in {@code pom.xml}. Quarkus' {@code
 * QuarkusTestProfileAwareClassOrderer} calls it first and keeps its order for integration tests.
 */
public class TestResourceGroupingClassOrderer implements ClassOrderer {

  private static final Set<String> SHARED_RESOURCES =
      Set.of(describe(DseTestResource.class, TestResourceScope.MATCHING_RESOURCES));

  @Override
  public void orderClasses(ClassOrdererContext context) {
    context
        .getClassDescriptors()
        .sort(
            Comparator.comparing((ClassDescriptor d) -> groupKey(d.getTestClass()))
                .thenComparing(d -> d.getTestClass().getName()));
  }

  /** "0" for the classes that share the default resources, so they run first. */
  private static String groupKey(Class<?> testClass) {
    var resources = testResources(testClass);
    return resources.equals(SHARED_RESOURCES) ? "0" : "1" + resources;
  }

  /**
   * The test resources of the class, declared on it, its superclasses or its enclosing classes,
   * like {@code TestResourceManager} finds them. Global resources apply to every class, so they are
   * left out.
   */
  private static Set<String> testResources(Class<?> testClass) {
    List<Class<?>> declaringClasses = new ArrayList<>();
    for (Class<?> c = testClass; c != null && c != Object.class; c = c.getSuperclass()) {
      declaringClasses.add(c);
    }
    for (Class<?> c = testClass.getEnclosingClass(); c != null; c = c.getEnclosingClass()) {
      declaringClasses.add(c);
    }

    var resources = new TreeSet<String>();
    for (Class<?> c : declaringClasses) {
      for (var a : c.getDeclaredAnnotationsByType(WithTestResource.class)) {
        if (a.scope() != TestResourceScope.GLOBAL) {
          resources.add(describe(a.value(), a.scope()));
        }
      }
      for (var a : c.getDeclaredAnnotationsByType(QuarkusTestResource.class)) {
        if (a.restrictToAnnotatedClass()) {
          resources.add(describe(a.value(), TestResourceScope.RESTRICTED_TO_CLASS));
        }
      }
    }
    return resources;
  }

  private static String describe(Class<?> resource, TestResourceScope scope) {
    return resource.getName() + " " + scope;
  }
}
