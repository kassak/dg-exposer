package com.github.kassak.intellij.expose.counterpart;

import com.intellij.database.console.JdbcEngine;
import com.intellij.database.datagrid.DataAuditor;
import com.intellij.database.datagrid.DataConsumer;
import com.intellij.database.datagrid.DataProducer;
import com.intellij.database.datagrid.DataRequest;
import com.intellij.database.datagrid.DataRequest.CallRequest.Statement;
import com.intellij.openapi.Disposable;
import com.intellij.openapi.progress.ProcessCanceledException;
import com.intellij.openapi.util.Disposer;
import com.intellij.util.ArrayUtil;
import com.intellij.util.containers.ContainerUtil;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntObjectHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.concurrency.AsyncPromise;
import org.jetbrains.concurrency.Promise;
import org.jetbrains.concurrency.Promises;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class DGCursor implements Disposable {
  private final DataRequest.OwnerEx myOwner;
  private final JdbcEngine myEngine;
  private final AtomicReference<QueryData> myData = new AtomicReference<>();
  private volatile String prevQuery;

  public DGCursor(@NotNull DataRequest.OwnerEx owner, @NotNull JdbcEngine engine) {
    myOwner = owner;
    myEngine = engine;
  }

  public Promise<Void> execute(@Nullable String query, @NotNull List<Object> params) {
    if (query == null) query = prevQuery;
    else prevQuery = query;
    if (query == null) return Promises.rejectedPromise("Empty query");
    TIntObjectHashMap<Object> p = new TIntObjectHashMap<>();
    for (int i = 0; i < params.size(); i++) {
      p.put(i + 1, params.get(i));
    }

    DataRequest request = DataRequest.newCallRequest(myOwner, Collections.singletonList(new Statement(query, new TIntIntHashMap(), p)), null);
    QueryData data = new QueryData(request);
    resetQueries(data);
    request.getPromise().processed(data.query);
    request.getPromise().onError(e -> data.poison());
    producer().processRequest(request);
    return data.query;
  }

  private void resetQueries(@Nullable QueryData data) {
    QueryData prev = myData.getAndSet(data);
    if (data != null) {
      myOwner.getMessageBus().addConsumer(data);
      myOwner.getMessageBus().addAuditor(data);
    }
    if (prev == null) return;
    Disposer.dispose(prev);
    myEngine.cancelPendingRequests();
  }

  public boolean haveQuery() {
    return myData.get() != null;
  }

  @Nullable
  public Throwable fetchError() {
    QueryData data = myData.get();
    return data == null ? null : data.fetchError();
  }

  @NotNull
  public List<DataConsumer.Row> fetch(int limit) throws InterruptedException {
    QueryData data = myData.get();
    if (data == null) return Collections.emptyList();
    return data.fetch(limit);
  }

  @Nullable
  public List<DataConsumer.Column> getColumns() {
    QueryData data = myData.get();
    if (data == null) return null;
    return data.columns;
  }

  private DataProducer producer() {
    return myOwner.getMessageBus().getDataProducer();
  }

  @Override
  public void dispose() {
    resetQueries(null);
  }

  private static class QueryData extends DataAuditor.Adapter implements DataConsumer, Disposable {
    final DataRequest request;
    final AsyncPromise<Void> query = new AsyncPromise<>();
    final AtomicReference<Throwable> lastException = new AtomicReference<>();
    volatile List<DataConsumer.Column> columns;

    final BlockingQueue<DataConsumer.Row> buffer = new ArrayBlockingQueue<>(50);
    boolean finished;

    private QueryData(DataRequest request) {
      this.request = request;
    }

    @Override
    public void dispose() {

    }

    List<DataConsumer.Row> fetch(int limit) throws InterruptedException {
      List<DataConsumer.Row> res = ContainerUtil.newArrayListWithCapacity(limit == -1 ? 50 : limit);
      while ((limit == -1 || res.size() < limit) && !finished) {
        res.add(buffer.take());
        buffer.drainTo(res, limit - res.size());
        removePoisoned(res);
      }
      return res;
    }

    private void removePoisoned(List<DataConsumer.Row> res) {
      while (true) {
        int last = res.size() - 1;
        if (last >= 0 && DataConsumer.Row.toRealIdx(res.get(last)) == Integer.MIN_VALUE) {
          finished = true;
          res.remove(last);
        }
        else {
          break;
        }
      }
    }

    private void poison() {
      try {
        buffer.put(DataConsumer.Row.create(Integer.MIN_VALUE, ArrayUtil.EMPTY_OBJECT_ARRAY));
      }
      catch (InterruptedException e) {
        throw new ProcessCanceledException();
      }
    }

    @Nullable
    public Throwable fetchError() {
      return lastException.getAndSet(null);
    }

    @Override
    public void error(@NotNull DataRequest.Context context, @Nullable String message, @Nullable Throwable th) {
      if (context.request != request) return;
      lastException.set(message == null ? th : new RuntimeException(message, th));
    }

    @Override
    public void setColumns(@NotNull DataRequest.Context context, int i, Column[] columns, int i1) {
      if (context.request != request) return;
      this.columns = ContainerUtil.newArrayList(columns);
    }

    @Override
    public void addRows(@NotNull DataRequest.Context context, List<Row> list) {
      if (context.request != request) return;
      try {
        for (DataConsumer.Row row : list) {
          buffer.put(row);
        }
      }
      catch (InterruptedException e) {
        throw new ProcessCanceledException();
      }
    }

    @Override
    public void afterLastRowAdded(@NotNull DataRequest.Context context, int i) {
      if (context.request != request) return;
      poison();
    }
  }
}
