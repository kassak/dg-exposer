package com.github.kassak.intellij.expose;

import com.google.gson.stream.JsonWriter;
import com.intellij.database.dataSource.DataSourceStorage;
import com.intellij.database.dataSource.LocalDataSource;
import com.intellij.openapi.Disposable;
import com.intellij.openapi.application.Application;
import com.intellij.openapi.components.ComponentManager;
import com.intellij.openapi.components.ServiceManager;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Disposer;
import com.intellij.util.ObjectUtils;
import com.intellij.util.containers.ContainerUtil;
import com.intellij.util.containers.JBIterable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Map;

import static com.github.kassak.intellij.expose.DataGripExposerService.*;
import static com.github.kassak.intellij.expose.DataSourceHandler.descDataSource;

public class ProjectHandler implements Disposable {
  public static ProjectHandler getInstance(@Nullable Project project) {
    return project == null ? ServiceManager.getService(App.class) : ServiceManager.getService(project, ProjectHandler.class);
  }

  private final Project myProject;
  private final Map<String, DataSourceHandler> myDataSources = ContainerUtil.newHashMap();

  public ProjectHandler(@NotNull Application application, @Nullable Project project) {
    myProject = project;
    ComponentManager component = ObjectUtils.chooseNotNull(project, application);
    Disposer.register(component, this);
    component.getMessageBus().connect(component).subscribe(DataSourceStorage.TOPIC, new DataSourceStorage.Listener() {
      @Override
      public void dataSourceRemoved(@NotNull LocalDataSource dataSource) {
        synchronized (myDataSources) {
          myDataSources.remove(dataSource.getUniqueId());
        }
      }
    });
  }

  public static class App extends ProjectHandler {
    public App(@NotNull Application application) {
      super(application, null);
    }
  }

  @Override
  public void dispose() {

  }

  static String processDataSource(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int base, @NotNull String uuid) throws IOException {
    Project project = null;
    LocalDataSource dataSource = DataSourceStorage.getStorage().getDataSourceById(uuid);
    if (dataSource == null) {
      for (Project p: getAllProjects()) {
        dataSource = DataSourceStorage.getProjectStorage(p).getDataSourceById(uuid);
        if (dataSource != null) {
          project = p;
          break;
        }
      }
    }
    if (dataSource == null) return notFound(request, context);
    return getInstance(project).processDataSource(urlDecoder, request, context, base, dataSource);
  }

  static String processDescAllDataSources(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    return sendJson(ProjectHandler::descAllDataSources, request, context);
  }

  private String processDataSource(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int base, @NotNull LocalDataSource dataSource) throws IOException {
    if (isEnd(urlDecoder, base)) {
      return request.method() == HttpMethod.GET ? descDataSource(request, context, dataSource) : badRequest(request, context);
    }
    int next = proceedIfStartsWith(urlDecoder, base, "connections/");
    if (next != -1) return getOrCreateDataSourceHandler(dataSource).processConnections(urlDecoder, request, context, base);
    return badRequest(request, context);
  }

  private DataSourceHandler getOrCreateDataSourceHandler(@NotNull LocalDataSource dataSource) {
    String id = dataSource.getUniqueId();
    synchronized (myDataSources) {
      DataSourceHandler handler = myDataSources.get(id);
      if (handler != null) return handler;
      handler = new DataSourceHandler(myProject, dataSource);
      myDataSources.put(id, handler);
      Disposer.register(this, handler);
      return handler;
    }
  }


  private static void descDataSources(JsonWriter json, JBIterable<LocalDataSource> dataSources) throws IOException {
    json.beginArray();
    for (LocalDataSource dataSource: dataSources) {
      descDataSource(json, dataSource);
    }
    json.endArray();
  }

  private static void descAllDataSources(JsonWriter json) throws IOException {
    descDataSources(json, getAllDataSources());
  }

}
