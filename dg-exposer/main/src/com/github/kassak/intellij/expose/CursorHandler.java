package com.github.kassak.intellij.expose;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.intellij.database.dataSource.DatabaseConnection;
import com.intellij.database.util.JdbcUtil;
import com.intellij.openapi.Disposable;
import com.intellij.openapi.util.Ref;
import com.intellij.util.containers.ContainerUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static com.github.kassak.intellij.expose.DataGripExposerService.*;

class CursorHandler implements Disposable {
  private final UUID myUuid;
  private final DatabaseConnection myConnection;
  private PreparedStatement myStatement;
  private ResultSet myResultSet;

  CursorHandler(DatabaseConnection connection) {
    myUuid = UUID.randomUUID();
    myConnection = connection;
  }

  void descCursor(JsonWriter json) throws IOException {
    json.beginObject();
    json.name("uuid").value(myUuid.toString());
    json.endObject();
  }

  @NotNull
  UUID getUuid() {
    return myUuid;
  }

  String processCursor(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int base) throws IOException {
    if (equal(urlDecoder, base, "execute")) return processExecute(request, context);
    return badRequest(request, context);
  }

  private String processExecute(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    Ref<String> query = Ref.create();
    List<Object> params = ContainerUtil.newArrayList();
    parseExecRequest(request, query, params);
    try {
      if (!query.isNull()) {
        cleanup();
        myStatement = myConnection.prepareStatement(query.get());
      }
      else if (myStatement == null) {
        return badRequest(request, context);
      }
      else {
        cleanupResultSet();
      }
      for (int i = 0; i < params.size(); ++i) {
        myStatement.setObject(i, params.get(i));
      }
      int count = myStatement.execute() ? -1 : myStatement.getUpdateCount();
      return sendJson(json -> {
        json.beginObject();
        json.name("rowcount").value(count);
        json.endObject();
      }, request, context);
    }
    catch (SQLException e) {
      return sendError(e, request, context);
    }
  }

  private void parseExecRequest(@NotNull FullHttpRequest request, Ref<String> query, List<Object> params) throws IOException {
    readJson(json -> {
      json.beginObject();
      while (json.hasNext()) {
        String name = json.nextName();
        if ("operation".equals(name)) query.set(json.nextString());
        else if ("parameters".equals(name)) {
          json.beginArray();
          while (json.hasNext()) {
            params.add(parseParam(json));
          }
          json.endArray();
        }
      }
      json.endObject();
    }, request);
  }

  private Object parseParam(JsonReader json) throws IOException {
    String type = null;
    String val = null;
    json.beginObject();
    while (json.hasNext()) {
      String name = json.nextName();
      String value = json.nextString();
      if ("value".equals(name)) val = value;
      else if ("type".equals(name)) type = value;
      else throw new AssertionError("Unexpected: " + name);
    }
    json.endObject();
    //todo:
    return val;
  }

  @Override
  public void dispose() {
    cleanup();
  }

  private void cleanup() {
    JdbcUtil.closeStatementSafe(myStatement);
    myStatement = null;
  }

  private void cleanupResultSet() {
    JdbcUtil.closeResultSetSafe(myResultSet);
    myResultSet = null;
  }
}
