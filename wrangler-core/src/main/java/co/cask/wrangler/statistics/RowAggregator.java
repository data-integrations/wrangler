/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.wrangler.statistics;

import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class description here.
 */
public final class RowAggregator implements Aggregator<Row> {
  private int count;
  private ConcurrentLinkedQueue<Row> queue;
  private AtomicBoolean running = new AtomicBoolean(true);
  private Thread thread;
  private final Row result = new Row();

  public RowAggregator() {
    this.count = 0;
    this.queue = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void initialize() {
    Runnable worker = new Runnable() {
      @Override
      public void run() {
        while(running.get() || !queue.isEmpty()) {
          if(!queue.isEmpty()) {
            Row row = queue.poll();
            for(int i = 0; i < row.length(); ++i) {
              result.addOrSet(row.getColumn(i), row.getValue(i).toString());
            }
          } else {
            try {
              Thread.sleep(1L);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      }
    };
    thread = new Thread(worker);
    thread.start();
  }

  @Override
  public Void execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {
    for(Row row : rows) {
      queue.add(row);
    }
    return null;
  }

  @Override
  public Row get(long millis) {
    running.set(false);
    try {
      thread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return result;
  }

  @Override
  public void destroy() {

  }
}
