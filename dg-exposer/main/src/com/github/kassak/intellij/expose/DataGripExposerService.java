package com.github.kassak.intellij.expose;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.intellij.database.dataSource.DataSourceStorage;
import com.intellij.database.dataSource.LocalDataSource;
import com.intellij.ide.DataManager;
import com.intellij.notification.NotificationGroup;
import com.intellij.notification.NotificationType;
import com.intellij.openapi.actionSystem.ActionManager;
import com.intellij.openapi.actionSystem.ActionPlaces;
import com.intellij.openapi.actionSystem.AnAction;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.actionSystem.ex.ActionUtil;
import com.intellij.openapi.application.ApplicationManager;
import com.intellij.openapi.diagnostic.Logger;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.project.ProjectManager;
import com.intellij.openapi.project.ProjectManagerListener;
import com.intellij.openapi.util.io.BufferExposingByteArrayOutputStream;
import com.intellij.openapi.util.registry.Registry;
import com.intellij.openapi.util.text.StringUtil;
import com.intellij.util.ExceptionUtil;
import com.intellij.util.ThrowableConsumer;
import com.intellij.util.containers.ContainerUtil;
import com.intellij.util.containers.JBIterable;
import com.intellij.util.messages.MessageBusConnection;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.ide.RestService;

import javax.swing.event.HyperlinkEvent;
import java.io.IOException;

public class DataGripExposerService extends RestService {
  static final Logger LOG = Logger.getInstance(DataGripExposerService.class);
  private static final String SERVICE_NAME = "database";
  private static final String SERVICE_PREFIX = "/" + PREFIX + "/" + SERVICE_NAME + "/";
  private static final NotificationGroup NOTIFICATION_GROUP = NotificationGroup.balloonGroup("DataGrip Exposer");
  private static final String REQUESTS_PROP = "ide.rest.api.requests.per.minute";

  public DataGripExposerService() {
    checkRequests();
  }

  @NotNull
  @Override
  protected String getServiceName() {
    return SERVICE_NAME;
  }

  @Override
  protected boolean isMethodSupported(@NotNull HttpMethod method) {
    return method == HttpMethod.GET || method == HttpMethod.POST || method == HttpMethod.DELETE;
  }

  @Nullable
  @Override
  public String execute(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    assert startsWith(urlDecoder, 0, SERVICE_PREFIX);
    int base = SERVICE_PREFIX.length();
    int next = proceedIfStartsWith(urlDecoder, base, "dataSources/");
    if (next != -1) return processDataSources(urlDecoder, request, context, next);
    return badRequest(request, context);
  }

  private String processDataSources(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int base) throws IOException {
    if (isEnd(urlDecoder, base)) {
      return request.method() == HttpMethod.GET ? ProjectHandler.processDescAllDataSources(request, context) : badRequest(request, context);
    }
    String dataSourceId = extractItem(urlDecoder, base);
    if (dataSourceId != null) {
      return ProjectHandler.processDataSource(urlDecoder, request, context, base + dataSourceId.length() + 1, dataSourceId);
    }
    return badRequest(request, context);
  }

  @NotNull
  static JBIterable<LocalDataSource> getAllDataSources() {
    return getAllProjects()
      .flatten(p -> JBIterable.from(DataSourceStorage.getProjectStorage(p).getDataSources()).filter(ds -> !ds.isGlobal()))
      .append(DataSourceStorage.getStorage().getDataSources());
  }

  @NotNull
  static JBIterable<Project> getAllProjects() {
    return JBIterable.of(ProjectManager.getInstance().getOpenProjects()).filter(p -> !p.isDefault() && p.isInitialized() && p.isOpen());
  }

  @NotNull
  static Project getAnyProjects() {
    Project p = getAllProjects().first();
    if (p != null) return p;
    return ProjectManager.getInstance().getDefaultProject();
  }


  static void readJson(@NotNull ThrowableConsumer<JsonReader, IOException> reader, @NotNull FullHttpRequest request) throws IOException {
    try (JsonReader json = createJsonReader(request)) {
      reader.consume(json);
    }
  }

  static String sendJson(@NotNull ThrowableConsumer<JsonWriter, IOException> writer, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) {
    try {
      BufferExposingByteArrayOutputStream byteOut = new BufferExposingByteArrayOutputStream();
      try (JsonWriter json = createJsonWriter(byteOut)) {
        writer.consume(json);
      }
      send(byteOut, request, context);
    }
    catch (Exception e) {
      LOG.error(e);
      sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR, HttpUtil.isKeepAlive(request), context.channel());
    }
    return null;
  }

  static String reportOk(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) {
    return sendJson(jsonWriter -> {
      jsonWriter.beginObject();
      jsonWriter.endObject();
    }, request, context);
  }

  static String notFound(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) {
    sendStatus(HttpResponseStatus.NOT_FOUND, HttpUtil.isKeepAlive(request), context.channel());
    return null;
  }

  static String badRequest(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) {
    sendStatus(HttpResponseStatus.BAD_REQUEST, HttpUtil.isKeepAlive(request), context.channel());
    return null;
  }

  static String sendError(@NotNull Throwable e, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, @Nullable String kind) {
    return sendError(e.getMessage(), e, request, context, kind);
  }

  static String sendError(@NotNull Throwable e, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) {
    return sendError(e, request, context, null);
  }

  static String sendError(@NotNull String msg, @Nullable Throwable e, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, @Nullable String kind) {
    return sendJson(json -> {
      json.beginObject();
      json.name("error").value(msg);
      if (e != null) json.name("trace").value(ExceptionUtil.getThrowableText(e));
      if (kind != null) json.name("kind").value(kind);
      json.endObject();
    }, request, context);
  }

  static boolean isEnd(@NotNull QueryStringDecoder urlDecoder, int base) {
    return urlDecoder.path().length() == base;
  }

  static int proceedIfStartsWith(@NotNull QueryStringDecoder urlDecoder, int offs, String frag) {
    return startsWith(urlDecoder, offs, frag) ? offs + frag.length() : -1;
  }

  private static boolean startsWith(@NotNull QueryStringDecoder urlDecoder, int offs, String frag) {
    return StringUtil.startsWith(urlDecoder.path(), offs, frag);
  }

  static boolean equal(@NotNull QueryStringDecoder urlDecoder, int offs, String frag) {
    return urlDecoder.path().length() == offs + frag.length() && startsWith(urlDecoder, offs, frag);
  }

  @Nullable
  static String extractItem(@NotNull QueryStringDecoder urlDecoder, int offs) {
    int i = urlDecoder.path().indexOf('/', offs);
    return i != -1 ? urlDecoder.path().substring(offs, i) : null;
  }


  private void checkRequests() {
    int req = Registry.intValue(REQUESTS_PROP, 30);
    if (req >= 300) return;
    ProjectManager manager = ProjectManager.getInstance();
    Project project = ContainerUtil.find(manager.getOpenProjects(), p -> !p.isDefault() && p.isInitialized() && p.isOpen());
    if (project != null) {
      notifySolution(project, req);
    }
    else {
      MessageBusConnection connection = ApplicationManager.getApplication().getMessageBus().connect();
      connection.subscribe(ProjectManager.TOPIC, new ProjectManagerListener() {
        @Override
        public void projectOpened(Project project) {
          connection.disconnect();
          notifySolution(project, req);
        }
      });
    }

  }

  private void notifySolution(Project p, int req) {
    NOTIFICATION_GROUP.createNotification(
      "DataGrip Exposer",
      "You have low limit of REST requests.\n" +
        "Limit of " + req + " requests per minute is too low.\n" +
        "You will experience problems with DB connectivity.\n" +
        "Set to <a href='set'>300</a>\n" +
        "<a href='edit'>Edit</a>",
      NotificationType.ERROR,
      (notification, event) -> {
        if (event.getEventType() == HyperlinkEvent.EventType.ACTIVATED) {
          if ("set".equals(event.getDescription())) {
            Registry.get(REQUESTS_PROP).setValue(300);
          } else {
            AnAction showRegistry = ActionManager.getInstance().getAction("ShowRegistry");
            if (showRegistry == null) return;
            ActionUtil.performActionDumbAware(
              showRegistry,
              AnActionEvent.createFromDataContext(ActionPlaces.UNKNOWN, null, DataManager.getInstance().getDataContext()));
          }
        }
      }
    ).notify(p);
  }
}
