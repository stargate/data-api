package io.stargate.sgv2.jsonapi.api.model.command.validation;

import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class FindOptionsValidation implements ConstraintValidator<CheckFindOption, FindCommand> {

  /** {@inheritDoc} */
  @Override
  public boolean isValid(FindCommand value, ConstraintValidatorContext context) {
    final FindCommand.Options options = value.options();
    context.disableDefaultConstraintViolation();
    if (options == null) return true;
    if (options.skip() != null && value.sortClause() == null) {
      context
          .buildConstraintViolationWithTemplate("skip options should be used with sort clause")
          .addPropertyNode("options.skip")
          .addConstraintViolation();
      return false;
    }
    if (options.skip() != null && options.skip() < 0) {
      context
          .buildConstraintViolationWithTemplate("skip should be grater than or equal to `0`")
          .addPropertyNode("options.skip")
          .addConstraintViolation();
      return false;
    }
    if (options.limit() != null && options.limit() < 0) {
      context
          .buildConstraintViolationWithTemplate("limit should be grater than or equal to `0`")
          .addPropertyNode("options.limit")
          .addConstraintViolation();
      return false;
    }
    if (options.pagingState() != null && value.sortClause() != null) {
      context
          .buildConstraintViolationWithTemplate("pagingState is not supported with sort clause")
          .addPropertyNode("options.pagingState")
          .addConstraintViolation();
      return false;
    }
    return true;
  }
}
