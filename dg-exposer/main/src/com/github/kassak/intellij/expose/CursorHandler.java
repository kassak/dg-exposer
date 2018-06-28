package com.github.kassak.intellij.expose;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.intellij.database.dataSource.DatabaseConnection;
import com.intellij.database.util.JdbcUtil;
import com.intellij.openapi.Disposable;
import com.intellij.openapi.util.Ref;
import com.intellij.openapi.util.text.StringUtil;
import com.intellij.util.containers.ContainerUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.UUID;

import static com.github.kassak.intellij.expose.DataGripExposerService.*;

class CursorHandler implements Disposable {
  private final UUID myUuid;
  private final DatabaseConnection myConnection;
  private PreparedStatement myStatement;
  private ResultSet myResultSet;
  private boolean myHasData;

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
    if (equal(urlDecoder, base, "execute")) return request.method() == HttpMethod.POST ? processExecute(request, context) : badRequest(request, context);
    if (equal(urlDecoder, base, "fetch")) return request.method() == HttpMethod.GET ? processFetch(urlDecoder, request, context) : badRequest(request, context);
    if (equal(urlDecoder, base, "nextSet")) return request.method() == HttpMethod.POST ? processNextSet(request, context) : badRequest(request, context);
    if (equal(urlDecoder, base, "describe")) return request.method() == HttpMethod.GET ? processDescribe(request, context) : badRequest(request, context);
    return badRequest(request, context);
  }

  private String processExecute(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    Ref<String> query = Ref.create();
    List<Object> params = ContainerUtil.newArrayList();
    parseExecRequest(request, query, params);
    try {
      if (!query.isNull()) {
        cleanup();
        myStatement = myConnection.getJdbcConnection().prepareStatement(query.get());
      }
      else if (myStatement == null) {
        return badRequest(request, context);
      }
      else {
        cleanupResultSet();
      }
      for (int i = 0; i < params.size(); ++i) {
        myStatement.setObject(i + 1, params.get(i));
      }
      int count = myStatement.execute() ? -1 : myStatement.getUpdateCount();
      storeResultSet();
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

  private void storeResultSet() throws SQLException {
    myResultSet = myStatement.getResultSet();
    myHasData = true;
  }

  private String processNextSet(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    if (myResultSet == null) return badRequest(request, context);
    cleanupResultSet();
    try {
      boolean more = myStatement.getMoreResults();
      if (more) storeResultSet();
      return sendJson(json -> {
        json.beginObject();
        json.name("more").value(more);
        json.endObject();
      }, request, context);
    }
    catch (SQLException e) {
      return sendError(e, request, context);
    }
  }

  private String processDescribe(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    try {
      return sendJson(json -> {
        try {
          describe(json);
        }
        catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }, request, context);
    }
    catch (RuntimeException e) {
      return sendError(e, request, context);
    }
  }

  private void describe(JsonWriter json) throws SQLException, IOException {
    ResultSetMetaData metaData = myResultSet == null || myResultSet.isClosed() ? null : myResultSet.getMetaData();
    json.beginArray();
    int count = metaData == null ? 0 : metaData.getColumnCount();
    for (int i = 0; i < count; ++i) {
      describeColumn(json, metaData, i);
    }
    json.endArray();
  }

  private void describeColumn(JsonWriter json, ResultSetMetaData metaData, int i) throws IOException, SQLException {
    json.beginObject();
    json.name("name").value(metaData.getColumnName(i + 1));
    json.name("type").value(getType(metaData.getColumnType(i + 1)));
    json.name("precision").value(metaData.getPrecision(i + 1));
    json.name("scale").value(metaData.getScale(i + 1));
    json.endObject();
  }

  private String getType(int type) {
    if (JdbcUtil.isNumberType(type) || type == Types.SMALLINT) return "N";
    if (JdbcUtil.isStringType(type)) return "S";
    if (JdbcUtil.isDateTimeType(type)) return "D";
    return "B";
  }

  private String processFetch(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    String limitStr = ContainerUtil.getLastItem(urlDecoder.parameters().get("limit"));
    int limit = -1;
    if (limitStr != null) {
      try {
        limit = Integer.parseInt(limitStr);
      }
      catch (NumberFormatException e) {
        return sendError(e, request, context);
      }
    }
    return processFetch(request, context, limit);
  }

  private String processFetch(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int limit) throws IOException {
    if (myResultSet == null) return badRequest(request, context);
    try {
      return sendJson(json -> {
        try {
          serializeResultSet(json, limit);
        }
        catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }, request, context);
    }
    catch (RuntimeException e) {
      return sendError(e, request, context);
    }
  }

  private void serializeResultSet(JsonWriter json, int limit) throws SQLException, IOException {
    int count = 0;
    int cols = -1;
    json.beginArray();
    while ((limit == -1 || count < limit) && myHasData && (myHasData = myResultSet.next())) {
      if (cols == -1) cols = myResultSet.getMetaData().getColumnCount();
      json.beginArray();
      for (int i = 0; i < cols; ++i) {
        serializeValue(json, i);
      }
      json.endArray();
      ++count;
    }
    if (!myHasData && myResultSet != null) cleanupResultSet();
    json.endArray();
  }

  private void serializeValue(JsonWriter json, int i) throws IOException, SQLException {
    json.value(myResultSet.getString(i + 1));
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
    return parseParam(val, type);
  }

  private Object parseParam(String val, String type) {
    if (type == null || val == null || "S".equals(type) || "B".equals(type)) return val;
    if ("N".equals(type)) {
      boolean fl = StringUtil.containsAnyChar(val, ".,ef");
      try {
        return fl ? Double.parseDouble(val) : Integer.parseInt(val);
      }
      catch (NumberFormatException e) {
        return val;
      }
    }
    if ("D".equals(type)) {
      try {
        return LocalDateTime.parse(val);
      }
      catch (DateTimeParseException e) {
        return val;
      }
    }
    return null;
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
