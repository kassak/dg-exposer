package com.github.kassak.intellij.expose;

import com.google.gson.stream.JsonWriter;
import com.intellij.database.dataSource.DatabaseConnection;
import com.intellij.database.dataSource.DatabaseConnectionManager;
import com.intellij.database.util.GuardedRef;
import com.intellij.openapi.Disposable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.UUID;

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

  @Override
  public void dispose() {
    if (DatabaseConnectionManager.getInstance().getActiveConnections().contains(getConnection())) {
      myConnection.close();
    }
  }
}
