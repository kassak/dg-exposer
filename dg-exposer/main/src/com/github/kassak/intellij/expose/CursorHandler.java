package com.github.kassak.intellij.expose;

import com.github.kassak.intellij.expose.counterpart.DGCursor;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.intellij.database.datagrid.DataConsumer;
import com.intellij.database.util.JdbcUtil;
import com.intellij.openapi.Disposable;
import com.intellij.openapi.util.Disposer;
import com.intellij.openapi.util.Ref;
import com.intellij.openapi.util.text.StringUtil;
import com.intellij.util.containers.ContainerUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.concurrency.Promise;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.UUID;

import static com.github.kassak.intellij.expose.DataGripExposerService.*;

class CursorHandler implements Disposable {
  private final UUID myUuid;
  private final DGCursor myCursor;
  private boolean myHasData;

  CursorHandler(DGCursor cursor) {
    myUuid = UUID.randomUUID();
    myCursor = cursor;
    Disposer.register(this, myCursor);
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
    try {
      parseExecRequest(request, query, params);
    }
    catch (Exception e) {
      return sendError(e, request, context);
    }
    Promise<Void> promise = myCursor.execute(query.get(), params);
    promise.onError(e -> {
      if (!reportError(request, context)) {
        sendError(e, request, context);
      }
    });
    promise.onSuccess(ignore -> sendJson(json -> {
      json.beginObject();
      json.name("rowcount").value(-1);//todo
      json.endObject();
    }, request, context));
    return null;
  }

//  private void storeResultSet() throws SQLException {
//    myResultSet = myStatement.getResultSet();
//    myHasData = true;
//  }

  private String processNextSet(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    if (reportError(request, context)) return null;
//    if (!myCursor.haveQuery()) return badRequest(request, context);
//    cleanupResultSet();
//    try {
//      boolean more = myStatement.getMoreResults();
//      if (more) storeResultSet();
//      return sendJson(json -> {
//        json.beginObject();
//        json.name("more").value(more);
//        json.endObject();
//      }, request, context);
//    }
//    catch (SQLException e) {
//      return sendError(e, request, context);
//    }
    return sendJson(json -> {
      json.beginObject();
      json.name("more").value(false);
      json.endObject();
    }, request, context);
  }

  private String processDescribe(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    if (reportError(request, context)) return null;
    return sendJson(this::describe, request, context);
  }

  private void describe(JsonWriter json) throws IOException {
    List<DataConsumer.Column> columns = myCursor.getColumns();
    json.beginArray();
    if (columns != null) {
      for (DataConsumer.Column column : columns) {
        describeColumn(json, column);
      }
    }
    json.endArray();
  }

  private void describeColumn(JsonWriter json, DataConsumer.Column column) throws IOException {
    json.beginObject();
    json.name("name").value(column.name);
    json.name("type").value(getType(column.type));
    json.name("precision").value(column.precision);
    json.name("scale").value(column.scale);
    json.endObject();
  }

  private String getType(int type) {
    if (type == Types.BIGINT || type == Types.INTEGER || type == Types.SMALLINT) return "I";
    if (JdbcUtil.isNumberType(type)) return "N";
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
    if (reportError(request, context)) return null;
    if (!myCursor.haveQuery()) return badRequest(request, context);
    try {
      return sendJson(json -> {
        try {
          serializeResultSet(json, limit);
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }, request, context);
    }
    catch (RuntimeException e) {
      return sendError(e, request, context);
    }
  }

  private boolean reportError(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) {
    Throwable err = myCursor.fetchError();
    if (err == null) return false;
    sendError(err, request, context);
    return true;
  }

  private void serializeResultSet(JsonWriter json, int limit) throws IOException, InterruptedException {
    json.beginArray();
    List<DataConsumer.Row> rows = myCursor.fetch(limit);
    if (limit == -1 || rows.size() < limit) myHasData = false;
    for (DataConsumer.Row row : rows) {
      json.beginArray();
      for (Object value : row.values) {
        serializeValue(json, value);
      }
      json.endArray();
    }
    json.endArray();
  }

  private void serializeValue(JsonWriter json, Object value) throws IOException {
    json.value(value == null ? null : value.toString());
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
      String value = getOptString(json);
      if ("value".equals(name)) val = value;
      else if ("type".equals(name)) type = value;
      else throw new AssertionError("Unexpected: " + name);
    }
    json.endObject();
    return parseParam(val, type);
  }

  private String getOptString(JsonReader json) throws IOException {
    if (json.peek() != JsonToken.NULL) return json.nextString();
    json.nextNull();
    return null;
  }

  private Object parseParam(String val, String type) {
    if (type == null || val == null || "S".equals(type) || "B".equals(type)) return val;
    if ("I".equals(type)) return parseInt(val);
    if ("N".equals(type)) return parseFloat(val);
    if ("D".equals(type)) return parseDate(val);
    return null;
  }

  @Nullable
  private Object parseDate(String val) {
    try {
      return LocalDateTime.parse(val);
    }
    catch (DateTimeParseException e) {
      return val;
    }
  }

  @NotNull
  private Object parseFloat(String val) {
    boolean fl = StringUtil.containsAnyChar(val, ".,ef");
    try {
      return fl ? Double.parseDouble(val) : Integer.parseInt(val);
    }
    catch (NumberFormatException e) {
      return val;
    }
  }

  @Nullable
  private Object parseInt(String val) {
    try {
      return Integer.parseInt(val);
    }
    catch (NumberFormatException e) {
      try {
        return new BigInteger(val);
      }
      catch (NumberFormatException e2) {
        return val;
      }
    }
  }

  @Override
  public void dispose() {
  }
}
