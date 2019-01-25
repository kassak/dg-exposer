package com.github.kassak.intellij.expose;

import com.intellij.testFramework.PlatformTestCase;
import com.intellij.util.TimeoutUtil;
import org.jetbrains.ide.BuiltInServerManager;

public class TestApp extends PlatformTestCase {
  public void testApp() {
    System.out.println("TestApp is running on port: " + BuiltInServerManager.getInstance().getPort());
    System.out.flush();
    TimeoutUtil.sleep(5_000_000);
  }
}
