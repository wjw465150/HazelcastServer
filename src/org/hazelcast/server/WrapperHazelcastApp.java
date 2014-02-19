package org.hazelcast.server;

import org.tanukisoftware.wrapper.WrapperManager;
import org.tanukisoftware.wrapper.WrapperSimpleApp;

public class WrapperHazelcastApp extends WrapperSimpleApp {
  @Override
  public Integer start(String[] args) {
    Integer result = super.start(args);

    WrapperManager.log(WrapperManager.WRAPPER_LOG_LEVEL_FATAL, "Started Wrapper HazelcastServer!");

    return result;
  }

  @Override
  public int stop(int exitCode) {
    int result = super.stop(exitCode);

    WrapperManager.log(WrapperManager.WRAPPER_LOG_LEVEL_FATAL, "Stoped Wrapper HazelcastServer!");

    return result;
  }

  protected WrapperHazelcastApp(String[] strings) {
    super(strings);
  }

  public static void main(String args[]) {
    new WrapperHazelcastApp(args);
  }

}
