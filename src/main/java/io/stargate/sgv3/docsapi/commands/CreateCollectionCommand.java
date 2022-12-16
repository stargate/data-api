package io.stargate.sgv3.docsapi.commands;

public class CreateCollectionCommand extends SchemaModificationCommand {
  public final String name;

  public CreateCollectionCommand(String name) {
    super();
    this.name = name;
  }

  @Override
  public boolean valid() {
    return true;
  }

  @Override
  public void validate() throws Exception {}
}
