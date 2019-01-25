package com.github.kassak.intellij.expose.counterpart;

import com.intellij.database.Dbms;
import com.intellij.database.console.JdbcEngine;
import com.intellij.database.datagrid.DataConsumer;
import com.intellij.database.datagrid.DataProducer;
import com.intellij.database.datagrid.DataRequest;
import com.intellij.database.dump.DumpRequest;
import com.intellij.database.extractors.DataExtractor;
import com.intellij.database.util.CharOut;
import com.intellij.openapi.Disposable;
import com.intellij.openapi.progress.ProcessCanceledException;
import com.intellij.util.ArrayUtil;
import com.intellij.util.containers.ContainerUtil;
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

public class DGCursor implements DataExtractor, Disposable {
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
    QueryData data = new QueryData();
    resetQueries(data);
    DumpRequest request = new DumpRequest(
      myOwner, query,
      DataRequest.newConstraints(1, -1, 0),
      null, this,
      myEngine.getDataSource().getDbms(),
      data, null) {
    };
    request.getPromise().processed(data.query);
    request.getPromise().onError(e -> data.poison());
    producer().processRequest(request);
    return data.query;
  }

  private void resetQueries(@Nullable QueryData data) {
    QueryData prev = myData.getAndSet(data);
    if (prev == null) return;
    myEngine.cancelPendingRequests();
  }

  public boolean haveQuery() {
    return myData.get() != null;
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

  @NotNull
  @Override
  public String getFileExtension() {
    return ".py";
  }

  @Override
  public Extraction startExtraction(CharOut out, Dbms dbms, boolean forceSkipHeader, List<DataConsumer.Column> allColumns, int... selectedColumns) {
    QueryData data = (QueryData)out;
    data.columns = allColumns;
    data.query.setResult(null);
    return data;
  }

  @Override
  public void dispose() {
    resetQueries(null);
  }

  private static class QueryData implements CharOut, Extraction {
    final AsyncPromise<Void> query = new AsyncPromise<>();
    volatile List<DataConsumer.Column> columns;

    final BlockingQueue<DataConsumer.Row> buffer = new ArrayBlockingQueue<>(50);
    boolean finished;


    @NotNull
    @Override
    public CharOut append(@NotNull CharSequence charSequence) {
      return this;
    }
    @Override
    public long length() {
      return 0;
    }
    @Nullable
    @Override
    public <T> T tryCast(Class<T> aClass) {
      return null;
    }

    @Override
    public void addData(List<DataConsumer.Row> list) {
      try {
        for (DataConsumer.Row row : list) {
          buffer.put(row);
        }
      }
      catch (InterruptedException e) {
        throw new ProcessCanceledException();
      }
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

    @Override
    public void complete() {
      poison();
    }

    private void poison() {
      try {
        buffer.put(DataConsumer.Row.create(Integer.MIN_VALUE, ArrayUtil.EMPTY_OBJECT_ARRAY));
      }
      catch (InterruptedException e) {
        throw new ProcessCanceledException();
      }
    }
  }
}
