package com.github.kassak.intellij.expose;

import com.google.gson.stream.JsonWriter;
import com.intellij.database.dataSource.DatabaseConnection;
import com.intellij.database.dataSource.DatabaseConnectionManager;
import com.intellij.database.util.GuardedRef;
import com.intellij.openapi.Disposable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

import static com.github.kassak.intellij.expose.DataGripExposerService.*;

class ConnectionHandler implements Disposable {
  private final UUID myUuid;
  private final GuardedRef<DatabaseConnection> myConnection;

  ConnectionHandler(GuardedRef<DatabaseConnection> myConnection) {
    myUuid = UUID.randomUUID();
    this.myConnection = myConnection;
  }

  void descConnection(JsonWriter json) throws IOException {
    json.beginObject();
    json.name("uuid").value(myUuid.toString());
    json.endObject();
  }

  @NotNull
  UUID getUuid() {
    return myUuid;
  }

  DatabaseConnection getConnection() {
    return myConnection.get();
  }

  String processConnection(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int base) throws IOException {
    if (equal(urlDecoder, base, "commit")) return processCommit(request, context);
    if (equal(urlDecoder, base, "rollback")) return processRollback(request, context);
    return badRequest(request, context);
  }

  private String processCommit(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    try {
      myConnection.get().commit();
      return reportOk(request, context);
    }
    catch (SQLException e) {
      return sendError(e, request, context);
    }
  }

  private String processRollback(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    try {
      myConnection.get().rollback();
      return reportOk(request, context);
    }
    catch (SQLException e) {
      return sendError(e, request, context);
    }
  }

  @Override
  public void dispose() {
    if (DatabaseConnectionManager.getInstance().getActiveConnections().contains(getConnection())) {
      myConnection.close();
    }
  }
}
