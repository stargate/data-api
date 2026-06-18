package io.stargate.sgv2.jsonapi.service.schema.tables;

/**
 * Basic rules that only have a {@link DefaultTypeBindingRule#isSupported()} flag.
 *
 * <p><b>Important:</b> The static fields {@link #ALL_SUPPORTED} and {@link #NONE_SUPPORTED} use
 * lazy initialization holder classes to prevent static initialization order issues. This ensures
 * that these fields are only initialized when first accessed, after the {@code
 * DefaultTypeBindingRules} class and all its dependencies (such as {@code TypeBindingPoint} enum)
 * are fully initialized.
 *
 * <p>This pattern is necessary because these instances are referenced during the static
 * initialization of other classes (like {@code ApiDataTypeDefs}), and the order in which classes
 * are loaded and initialized can vary depending on application startup sequence and dependency
 * injection timing. Without lazy initialization, accessing these fields during static
 * initialization of dependent classes could result in {@code NullPointerException} or {@code
 * ExceptionInInitializerError}.
 *
 * <p>The holder pattern ensures thread-safe, lazy initialization that is resilient to different
 * class loading orders.
 */
public class DefaultTypeBindingRules
    extends BindingPointRules<DefaultTypeBindingRules.DefaultTypeBindingRule> {

  /**
   * Holder class for lazy initialization of {@link #ALL_SUPPORTED}. The inner class is only
   * initialized when {@link #ALL_SUPPORTED} is first accessed, ensuring all dependencies are ready.
   */
  private static class AllSupportedHolder {
    static final DefaultTypeBindingRules INSTANCE =
        new DefaultTypeBindingRules(
            createAll(TypeBindingPoint.COLLECTION_VALUE),
            createAll(TypeBindingPoint.MAP_KEY),
            createAll(TypeBindingPoint.TABLE_COLUMN),
            createAll(TypeBindingPoint.UDT_FIELD));
  }

  /**
   * Re-usable rules that the type is supported in all binding points. Uses lazy initialization to
   * prevent static initialization order issues.
   */
  public static final DefaultTypeBindingRules ALL_SUPPORTED = AllSupportedHolder.INSTANCE;

  /**
   * Holder class for lazy initialization of {@link #NONE_SUPPORTED}. The inner class is only
   * initialized when {@link #NONE_SUPPORTED} is first accessed, ensuring all dependencies are
   * ready.
   */
  private static class NoneSupportedHolder {
    static final DefaultTypeBindingRules INSTANCE =
        new DefaultTypeBindingRules(
            createNone(TypeBindingPoint.COLLECTION_VALUE),
            createNone(TypeBindingPoint.MAP_KEY),
            createNone(TypeBindingPoint.TABLE_COLUMN),
            createNone(TypeBindingPoint.UDT_FIELD));
  }

  /**
   * Re-usable rules that the type is not supported in any binding points. Uses lazy initialization
   * to prevent static initialization order issues.
   */
  public static final DefaultTypeBindingRules NONE_SUPPORTED = NoneSupportedHolder.INSTANCE;

  public DefaultTypeBindingRules(DefaultTypeBindingRule... rules) {
    super(rules);
  }

  public static DefaultTypeBindingRule create(
      TypeBindingPoint bindingPoint, boolean bindableFromDb, boolean bindableFromUser) {
    return new DefaultTypeBindingRule(bindingPoint, bindableFromDb, bindableFromUser);
  }

  public static DefaultTypeBindingRule createAll(TypeBindingPoint bindingPoint) {
    return new DefaultTypeBindingRule(bindingPoint, true, true);
  }

  public static DefaultTypeBindingRule createNone(TypeBindingPoint bindingPoint) {
    return new DefaultTypeBindingRule(bindingPoint, false, false);
  }

  public record DefaultTypeBindingRule(
      TypeBindingPoint bindingPoint, boolean bindableFromDb, boolean bindableFromUser)
      implements BindingPointRules.BindingPointRule {}
}
