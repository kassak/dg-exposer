package com.github.kassak.intellij.expose;

import com.intellij.database.dataSource.DataSourceStorage;
import com.intellij.database.dataSource.LocalDataSource;
import com.intellij.openapi.util.registry.Registry;
import com.intellij.testFramework.PlatformTestCase;
import com.intellij.testFramework.PlatformTestUtil;
import com.intellij.util.TimeoutUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.ide.BuiltInServerManager;
import org.jetbrains.ide.HttpRequestHandler;
import org.jetbrains.ide.RestService;

import java.awt.*;
import java.io.IOException;

public class TestApp extends PlatformTestCase {
  public void testApp() {
    Registry.get("ide.rest.api.requests.per.minute").setValue(3_000_000);
    PlatformTestUtil.registerExtension(HttpRequestHandler.Companion.getEP_NAME(), new RestService() {

      @NotNull
      @Override
      protected String getServiceName() {
        return "TestApp";
      }

      @Override
      protected boolean isMethodSupported(@NotNull HttpMethod httpMethod) {
        return true;
      }

      @Override
      public String execute(@NotNull QueryStringDecoder decoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
        sendOk(request, context);
        return null;
      }
    }, getTestRootDisposable());
    System.out.println("TestApp is running on port: " + BuiltInServerManager.getInstance().getPort());
    System.out.flush();
    LocalDataSource ds = new LocalDataSource("identifier.sqlite", "", "sqlite:identifier.sqlite", null, null);
    DataSourceStorage.getProjectStorage(getProject()).addDataSource(ds);
    SecondaryLoop loop = Toolkit.getDefaultToolkit().getSystemEventQueue().createSecondaryLoop();
    loop.enter();
  }
}
