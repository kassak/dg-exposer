package com.github.kassak.intellij.expose;

import com.github.kassak.intellij.expose.counterpart.DGCursor;
import com.google.gson.stream.JsonWriter;
import com.intellij.database.DataBus;
import com.intellij.database.SimpleRequestBroker;
import com.intellij.database.console.JdbcEngine;
import com.intellij.database.dataSource.LocalDataSource;
import com.intellij.database.datagrid.DataRequest;
import com.intellij.database.run.TxMarkerAuditor;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.github.kassak.intellij.expose.DataGripExposerService.*;

class ConnectionHandler implements DataRequest.OwnerEx, TxMarkerAuditor.TxMarkerHolder {
  private final UUID myUuid;
  private final JdbcEngine myEngine;
  private final Map<String, CursorHandler> myCursors = ContainerUtil.newHashMap();
  private final SimpleRequestBroker myBroker;
  private DataRequest.TxMarker myTxMarker = DataRequest.NONE;

  ConnectionHandler(@NotNull Project project, @NotNull LocalDataSource dataSource) {
    myUuid = UUID.randomUUID();
    myBroker = SimpleRequestBroker.newInstance(project, getDisplayName());
    myBroker.addAuditor(new TxMarkerAuditor(this));
    myEngine = createConnectionImpl(project, dataSource);
    Disposer.register(this, myEngine);
  }

  @NotNull
  private JdbcEngine createConnectionImpl(@NotNull Project project, @NotNull LocalDataSource dataSource) {
    JdbcEngine engine = new JdbcEngine(project, DataBus.shortCircuit(this.getMessageBus()), dataSource, null);
    return engine;
  }

  @Override
  public DataBus.Consuming getMessageBus() {
    return myBroker;
  }

  @NotNull
  @Override
  public DataRequest.TxMarker getCurrentTx() {
    return myTxMarker;
  }

  @Override
  public void setCurrentTx(@NotNull DataRequest.TxMarker txMarker) {
    myTxMarker = txMarker;
  }

  @Override
  public void setAutoCommit(boolean b) {
    throw new UnsupportedOperationException("not supported");
  }

  @NotNull
  @Override
  public String getDisplayName() {
    return "DG exposer";
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

  JdbcEngine getConnection() {
    return myEngine;
  }

  String processConnection(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int base) throws IOException {
    if (equal(urlDecoder, base, "commit")) return request.method() == HttpMethod.POST ? processCommit(request, context) : badRequest(request, context);
    if (equal(urlDecoder, base, "rollback")) return request.method() == HttpMethod.POST ? processRollback(request, context) : badRequest(request, context);
    int next = proceedIfStartsWith(urlDecoder, base, "cursors/");
    if (next != -1) return processCursors(urlDecoder, request, context, next);
    return badRequest(request, context);
  }

  private String processCursors(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int base) throws IOException {
    if (isEnd(urlDecoder, base)) {
      if (request.method() == HttpMethod.GET) return processDescCursors(request, context);
      else if (request.method() == HttpMethod.POST) return processCreateCursor(request, context);
      else return badRequest(request, context);
    }
    String cursorId = extractItem(urlDecoder, base);
    if (cursorId != null) return processCursor(urlDecoder, request, context, base + cursorId.length() + 1, cursorId);
    return badRequest(request, context);
  }

  private String processCursor(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int base, String cursorId) throws IOException {
    if (isEnd(urlDecoder, base)) {
      return request.method() == HttpMethod.DELETE ? processCloseCursor(request, context, cursorId) : badRequest(request, context);
    }
    CursorHandler handler;
    synchronized (myCursors){
      handler = myCursors.get(cursorId);
    }
    if (handler == null) return notFound(request, context);
    return handler.processCursor(urlDecoder, request, context, base);
  }

  private String processCloseCursor(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, @NotNull String cursorId) throws IOException {
    CursorHandler handler;
    synchronized (myCursors) {
      handler = myCursors.remove(cursorId);
    }
    if (handler == null) return notFound(request, context);
    Disposer.dispose(handler);
    return reportOk(request, context);
  }

  private String processDescCursors(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    List<CursorHandler> cursors;
    synchronized (myCursors) {
      cursors = ContainerUtil.newArrayList(myCursors.values());
    }
    return sendJson(json -> descCursors(json, cursors), request, context);
  }

  private void descCursors(JsonWriter json, List<CursorHandler> cursors) throws IOException {
    json.beginArray();
    for (CursorHandler cursor: cursors) {
      cursor.descCursor(json);
    }
    json.endArray();
  }

  private String processCreateCursor(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    try {
      CursorHandler handler = createCursor();
      if (handler == null) return badRequest(request, context);
      sendJson(handler::descCursor, request, context);
      return null;
    }
    catch (SQLException e) {
      return sendError(e, request, context);
    }
  }

  private CursorHandler createCursor() throws SQLException {
    CursorHandler handler = new CursorHandler(new DGCursor(this, myEngine));
    Disposer.register(this, handler);
    synchronized (myCursors) {
      myCursors.put(handler.getUuid().toString(), handler);
    }
    return handler;
  }


  private String processCommit(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) {
    return processSimpleRequest(request, context, DataRequest.newTxCommit(this));
  }

  @Nullable
  private String processSimpleRequest(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, DataRequest req) {
    req.getPromise().onSuccess(e -> reportOk(request, context));
    req.getPromise().onError(e -> sendError(e, request, context, "O"));
    myBroker.getDataProducer().processRequest(req);
    return null;
  }

  private String processRollback(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) {
    return processSimpleRequest(request, context, DataRequest.newTxRollback(this));
  }

  @Override
  public void dispose() {
  }
}
