package org.hazelcast.server;

import org.tanukisoftware.wrapper.WrapperSimpleApp;

public class WrapperHazelcastApp extends WrapperSimpleApp {
  protected WrapperHazelcastApp(String[] strings) {
    super(strings);
  }

  public static void main(String args[]) {
    new WrapperHazelcastApp(args);
  }

}
