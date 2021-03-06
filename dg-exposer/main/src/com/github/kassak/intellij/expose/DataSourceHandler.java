package com.github.kassak.intellij.expose;

import com.google.gson.stream.JsonWriter;
import com.intellij.database.DataBus;
import com.intellij.database.SimpleRequestBroker;
import com.intellij.database.console.JdbcEngine;
import com.intellij.database.dataSource.DatabaseConnection;
import com.intellij.database.dataSource.DatabaseConnectionManager;
import com.intellij.database.dataSource.LocalDataSource;
import com.intellij.database.datagrid.DataRequest;
import com.intellij.database.util.GuardedRef;
import com.intellij.openapi.Disposable;
import com.intellij.openapi.application.ApplicationManager;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Disposer;
import com.intellij.util.containers.ContainerUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.github.kassak.intellij.expose.DataGripExposerService.*;

class DataSourceHandler implements Disposable {
  private final Project myProject;
  private final LocalDataSource myDataSource;
  private final Map<String, ConnectionHandler> myConnections = ContainerUtil.newHashMap();

  public DataSourceHandler(@NotNull Project project, @NotNull LocalDataSource dataSource) {
    myProject = project;
    myDataSource = dataSource;
//    ApplicationManager.getApplication().getMessageBus().connect(this).subscribe(DatabaseConnectionManager.TOPIC, (c, add) -> {
//      if (!add) {
//        synchronized (myConnections) {
//          Iterator<Map.Entry<String, ConnectionHandler>> it = myConnections.entrySet().iterator();
//          while (it.hasNext()) {
//            Map.Entry<String, ConnectionHandler> e = it.next();
//            if (e.getValue().getConnection() == c) {
//              it.remove();
//              Disposer.dispose(e.getValue());
//            }
//          }
//        }
//      }
//    });
  }

  @Override
  public void dispose() {

  }

  @NotNull
  public LocalDataSource getDataSource() {
    return myDataSource;
  }

  static void descDataSource(JsonWriter json, LocalDataSource dataSource) throws IOException {
    json.beginObject();
    json.name("uuid").value(dataSource.getUniqueId());
    json.name("name").value(dataSource.getName());
    json.name("url").value(dataSource.getUrl());
    json.name("dbms").value(dataSource.getDbms().getName());
    json.endObject();
  }

  String processConnections(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int base) throws IOException {
    if (isEnd(urlDecoder, base)) {
      if (request.method() == HttpMethod.GET) return processDescConnections(request, context);
      else if (request.method() == HttpMethod.POST) return processCreateConnection(request, context);
      else return badRequest(request, context);
    }
    String connectionId = extractItem(urlDecoder, base);
    if (connectionId != null) return processConnection(urlDecoder, request, context, base + connectionId.length() + 1, connectionId);
    return badRequest(request, context);
  }

  private String processConnection(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int base, @NotNull String connectionId) throws IOException {
    if (isEnd(urlDecoder, base)) {
      return request.method() == HttpMethod.DELETE ? processCloseConnection(request, context, connectionId) : badRequest(request, context);
    }
    ConnectionHandler handler;
    synchronized (myConnections) {
      handler = myConnections.get(connectionId);
    }
    if (handler == null) return notFound(request, context);
    return handler.processConnection(urlDecoder, request, context, base);
  }

  static String descDataSource(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, @NotNull LocalDataSource dataSource) {
    return sendJson(json -> descDataSource(json, dataSource), request, context);
  }

  private String processCreateConnection(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) {
    try {
      ConnectionHandler handler = createConnection();
      if (handler == null) return badRequest(request, context);
      sendJson(handler::descConnection, request, context);
      return null;
    }
    catch (Exception e) {
      return sendError(e, request, context);
    }
  }

  private ConnectionHandler createConnection() {
    ConnectionHandler handler = new ConnectionHandler(getAnyProjects(), myDataSource);
    Disposer.register(this, handler);
    synchronized (myConnections) {
      myConnections.put(handler.getUuid().toString(), handler);
    }
    return handler;
  }

  private String processCloseConnection(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, @NotNull String connectionId) {
    ConnectionHandler handler;
    synchronized (myConnections) {
      handler = myConnections.remove(connectionId);
    }
    if (handler == null) return notFound(request, context);
    Disposer.dispose(handler);
    return reportOk(request, context);
  }

  private String processDescConnections(FullHttpRequest request, ChannelHandlerContext context) {
    List<ConnectionHandler> connections;
    synchronized (myConnections) {
      connections = ContainerUtil.newArrayList(myConnections.values());
    }
    return sendJson(json -> descConnections(json, connections), request, context);
  }

  private void descConnections(JsonWriter json, List<ConnectionHandler> connections) throws IOException {
    json.beginArray();
    for (ConnectionHandler connection: connections) {
      connection.descConnection(json);
    }
    json.endArray();
  }
}
