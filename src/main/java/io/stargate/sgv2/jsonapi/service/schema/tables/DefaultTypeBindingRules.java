package io.stargate.sgv2.jsonapi.service.schema.tables;

/** Basic rules that only have a {@link DefaultTypeBindingRule#isSupported()} flag. */
public class DefaultTypeBindingRules
    extends BindingPointRules<DefaultTypeBindingRules.DefaultTypeBindingRule> {

  /** Re-usable rules that the type is supported in all binding points. */
  public static final DefaultTypeBindingRules ALL_SUPPORTED =
      new DefaultTypeBindingRules(
          createAll(TypeBindingPoint.COLLECTION_VALUE),
          createAll(TypeBindingPoint.MAP_KEY),
          createAll(TypeBindingPoint.TABLE_COLUMN),
          createAll(TypeBindingPoint.UDT_FIELD));

  public static final DefaultTypeBindingRules NONE_SUPPORTED =
      new DefaultTypeBindingRules(
          createNone(TypeBindingPoint.COLLECTION_VALUE),
          createNone(TypeBindingPoint.MAP_KEY),
          createNone(TypeBindingPoint.TABLE_COLUMN),
          createNone(TypeBindingPoint.UDT_FIELD));

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
      implements BindingPointRule {}
}
