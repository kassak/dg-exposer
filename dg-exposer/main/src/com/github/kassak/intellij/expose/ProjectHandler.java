package com.github.kassak.intellij.expose;

import com.google.gson.stream.JsonWriter;
import com.intellij.credentialStore.OneTimeString;
import com.intellij.database.access.DatabaseCredentials;
import com.intellij.database.dataSource.DataSourceStorage;
import com.intellij.database.dataSource.DatabaseDriver;
import com.intellij.database.dataSource.LocalDataSource;
import com.intellij.database.dataSource.validation.DatabaseDriverValidator;
import com.intellij.database.dataSource.validation.NamedProgressive;
import com.intellij.database.util.DbImplUtil;
import com.intellij.openapi.Disposable;
import com.intellij.openapi.application.Application;
import com.intellij.openapi.components.ComponentManager;
import com.intellij.openapi.components.ServiceManager;
import com.intellij.openapi.progress.EmptyProgressIndicator;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Disposer;
import com.intellij.openapi.util.Ref;
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
          DataSourceHandler handler = myDataSources.remove(dataSource.getUniqueId());
          if (handler != null) Disposer.dispose(handler);
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

  String processCreateDataSource(@NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context) throws IOException {
    LocalDataSource newDs;
    try {
      newDs = parseAndAddDs(request);
    }
    catch (Throwable th) {
      return sendError(th, request, context);
    }
    return sendJson(json -> DataSourceHandler.descDataSource(json, newDs), request, context);
  }

  @NotNull
  private LocalDataSource parseAndAddDs(@NotNull FullHttpRequest request) throws IOException {
    Ref<LocalDataSource> ds = Ref.create();
    Ref<String> password = Ref.create();
    readJson(json -> {
      LocalDataSource res = new LocalDataSource();
      json.beginObject();
      while (json.hasNext()) {
        String key = json.nextName();
        if ("name".equals(key)) res.setName(json.nextString());
        else if ("url".equals(key)) res.setUrlSmart(json.nextString());
        else if ("user".equals(key)) res.setUsername(json.nextString());
        else if ("password".equals(key)) password.set(json.nextString());
      }
      json.endObject();
      ds.set(res);
    }, request);
    addDs(ds.get(), password.get());
    return ds.get();
  }

  private void addDs(@NotNull LocalDataSource ds, @Nullable String password) {
    ds.setPasswordStorage(LocalDataSource.Storage.PERSIST);
    DatabaseCredentials.getInstance().setPassword(ds, password == null ? null : new OneTimeString(password));
    ds.resolveDriver();
    ds.ensureDriverConfigured();
    downloadDrivers(ds); //todo
    DataSourceStorage.getProjectStorage(myProject).addDataSource(ds);
  }

  private void downloadDrivers(LocalDataSource ds) {
    if (DbImplUtil.hasDriverFiles(ds)) return;
    DatabaseDriver driver = ds.getDatabaseDriver();
    DatabaseDriver.ArtifactRef artifact = driver == null ? null : driver.getArtifact();
    if (artifact == null) return;
    NamedProgressive task = DatabaseDriverValidator.createDownloaderTask(ds, null);
    task.run(new EmptyProgressIndicator());
  }

  private String processDataSource(@NotNull QueryStringDecoder urlDecoder, @NotNull FullHttpRequest request, @NotNull ChannelHandlerContext context, int base, @NotNull LocalDataSource dataSource) throws IOException {
    if (isEnd(urlDecoder, base)) {
      if (request.method() == HttpMethod.GET) {
        return descDataSource(request, context, dataSource);
      }
      else if (request.method() == HttpMethod.DELETE) {
        return deleteDataSource(request, context, dataSource);
      }
      else return badRequest(request, context);
    }
    int next = proceedIfStartsWith(urlDecoder, base, "connections/");
    if (next != -1) return getOrCreateDataSourceHandler(dataSource).processConnections(urlDecoder, request, context, next);
    return badRequest(request, context);
  }

  private String deleteDataSource(FullHttpRequest request, ChannelHandlerContext context, LocalDataSource dataSource) {
    synchronized (myDataSources) {
      DataSourceHandler handler = myDataSources.remove(dataSource.getUniqueId());
      DataSourceStorage.getProjectStorage(myProject).removeDataSource(dataSource);
      if (handler != null) Disposer.dispose(handler);
    }
    return reportOk(request, context);
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
