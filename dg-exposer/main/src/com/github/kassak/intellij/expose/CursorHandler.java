package com.github.kassak.intellij.expose;

import com.github.kassak.intellij.expose.counterpart.DGCursor;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.intellij.database.datagrid.DataConsumer;
import com.intellij.database.extractors.ObjectFormatter;
import com.intellij.database.extractors.tz.TimeZonedTime;
import com.intellij.database.run.ui.grid.editors.DataGridFormattersUtil;
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
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
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
    json.name("type").value(MyType.getType(column).code);
    json.name("precision").value(column.precision);
    json.name("scale").value(column.scale);
    json.endObject();
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
          serializeResultSet(json, myCursor.getColumns(), limit);
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

  private void serializeResultSet(JsonWriter json, List<DataConsumer.Column> columns, int limit) throws IOException, InterruptedException {
    json.beginArray();
    List<DataConsumer.Row> rows = myCursor.fetch(limit);
    if (limit == -1 || rows.size() < limit) myHasData = false;
    for (DataConsumer.Row row : rows) {
      json.beginArray();
      Object[] values = row.values;
      for (int i = 0; i < values.length; i++) {
        Object value = values[i];
        DataConsumer.Column column = columns != null && i < columns.size() ? columns.get(i) : null;
        serializeValue(json, column, value);
      }
      json.endArray();
    }
    json.endArray();
  }

  private void serializeValue(JsonWriter json, DataConsumer.Column column, Object value) throws IOException {
    MyType type = value == null ? null : MyType.getType(column);
    if (type == null) {
      json.value(value == null ? null : value.toString());
    }
    else {
       type.serialize(json, value);
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
      String value = getOptString(json);
      if ("value".equals(name)) val = value;
      else if ("type".equals(name)) type = value;
      else throw new AssertionError("Unexpected: " + name);
    }
    json.endObject();
    return parseParam(val, MyType.getType(type));
  }

  private String getOptString(JsonReader json) throws IOException {
    if (json.peek() != JsonToken.NULL) return json.nextString();
    json.nextNull();
    return null;
  }

  private Object parseParam(String val, MyType type) {
    if (type == null || val == null) return val;
    return type.parse(val);
  }

  @Override
  public void dispose() {
  }

  private enum MyType {
    INT("I") {
      @Override
      Object parse(String val) {
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
    },
    BOOL("1") {
      @Override
      Object parse(String val) {
        try {
          return Integer.parseInt(val) == 1;
        }
        catch (NumberFormatException e) {
          return val;
        }
      }

      @Override
      public void serialize(JsonWriter json, Object val) throws IOException {
        if (val instanceof Boolean) {
          json.value((Boolean)val ? "1" : "0");
          return;
        }
        super.serialize(json, val);
      }
    },
    NUM("N") {
      @Override
      Object parse(String val) {
        boolean fl = StringUtil.containsAnyChar(val, ".,ef");
        try {
          return fl ? Double.parseDouble(val) : Integer.parseInt(val);
        }
        catch (NumberFormatException e) {
          return val;
        }
      }
    },
    STR("S"),
    DATE("D") {
      @Override
      Object parse(String val) {
        return parseDate(DATE_FORMATTER, val);
      }
    },
    TIME("T") {
      @Override
      Object parse(String val) {
        return parseDate(TIME_FORMATTER, val);
      }

      @Override
      public void serialize(JsonWriter json, Object val) throws IOException {
//        if (val instanceof TimeZonedTime) {
//          OffsetDateTime odt = DataGridFormattersUtil.fromTimestamp((TimeZonedTime) val);
//          json.value(DATE_TIME_FORMATTER.format(odt));
//          return;
//        }
        super.serialize(json, val);
      }
    },
    DATETIME("d") {
      @Override
      Object parse(String val) {
        return parseDate(DATE_TIME_FORMATTER, val);
      }

      @Override
      public void serialize(JsonWriter json, Object val) throws IOException {
        if (val instanceof Timestamp) {
          OffsetDateTime odt = DataGridFormattersUtil.fromTimestamp((Timestamp) val);
          json.value(DATE_TIME_FORMATTER.format(odt));
          return;
        }
        super.serialize(json, val);
      }
    },
    BIN("b");

    final String code;

    MyType(String code) {
      this.code = code;
    }

    static MyType getType(DataConsumer.Column column) {
      int type = column.type;
      if (type == Types.BIGINT || type == Types.INTEGER || type == Types.SMALLINT) return INT;
      if (ObjectFormatter.isBooleanColumn(column)) return BOOL;
      if (JdbcUtil.isNumberType(type) || type == Types.REAL) return NUM;
      if (JdbcUtil.isStringType(type)) return STR;
      if (type == Types.DATE) return DATE;
      if (type == Types.TIME) return TIME;
      if (JdbcUtil.isDateTimeType(type)) return DATETIME;
      return BIN;
    }

    static MyType getType(@Nullable String code) {
      for (MyType type : MyType.values()) {
        if (type.code.equals(code)) return type;
      }
      throw new AssertionError("unknown " + code);
    }

    Object parse(String val) {
      return val;
    }

    private static final DateTimeFormatter FRAC_FORMATTER = new DateTimeFormatterBuilder()
      .appendLiteral('.')
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9,true)
      .parseStrict()
      .toFormatter();
    private static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_TIME)
      .appendOptional(FRAC_FORMATTER)
      .toFormatter();
    private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .parseStrict()
      .toFormatter();
    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
      .append(DATE_FORMATTER)
      .appendLiteral(' ')
      .append(TIME_FORMATTER)
      .toFormatter();


    @Nullable
    protected Object parseDate(DateTimeFormatter fmt, String val) {
      try {
        return LocalDateTime.parse(val, fmt);
      }
      catch (DateTimeParseException e) {
        return val;
      }
    }

    public void serialize(JsonWriter json, Object val) throws IOException {
      json.value(val.toString());
    }
  }
}
