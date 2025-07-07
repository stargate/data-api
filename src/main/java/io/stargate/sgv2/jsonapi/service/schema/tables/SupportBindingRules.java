package io.stargate.sgv2.jsonapi.service.schema.tables;

/**
 * Basic rules that only have a {@link SupportBindingRule#isSupported()} flag.
 */
public class SupportBindingRules extends BindingPointRules<SupportBindingRules.SupportBindingRule> {

  /**
   * Re-usable rules that the type is supported in all binding points.
   */
  public static final SupportBindingRules ALL_SUPPORTED =
      new SupportBindingRules(
          createAll(TypeBindingPoint.COLLECTION_VALUE),
          createAll(TypeBindingPoint.MAP_KEY),
          createAll(TypeBindingPoint.TABLE_COLUMN),
          createAll(TypeBindingPoint.UDT_FIELD)
      );

  public static final SupportBindingRules NONE_SUPPORTED =
      new SupportBindingRules(
          createNone(TypeBindingPoint.COLLECTION_VALUE),
          createNone(TypeBindingPoint.MAP_KEY),
          createNone(TypeBindingPoint.TABLE_COLUMN),
          createNone(TypeBindingPoint.UDT_FIELD)
      );

  public SupportBindingRules(SupportBindingRule... rules) {
    super(rules);
  }

  public static SupportBindingRule create(
      TypeBindingPoint bindingPoint, boolean supportedFromDb, boolean supportedFromUser) {
    return new SupportBindingRule(bindingPoint, supportedFromDb, supportedFromUser);
  }


  public static SupportBindingRule createAll(TypeBindingPoint bindingPoint) {
    return new SupportBindingRule(bindingPoint, true, true);
  }

  public static SupportBindingRule createNone(TypeBindingPoint bindingPoint) {
    return new SupportBindingRule(bindingPoint, false, false);
  }

  public record SupportBindingRule(TypeBindingPoint bindingPoint, boolean supportedFromDb, boolean supportedFromUser)
      implements BindingPointRule {
  }
}
