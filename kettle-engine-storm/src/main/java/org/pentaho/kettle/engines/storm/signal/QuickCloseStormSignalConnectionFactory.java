/*
 * *****************************************************************************
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 * *****************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ****************************************************************************
 */

package org.pentaho.kettle.engines.storm.signal;

import backtype.storm.contrib.signals.SignalListener;
import backtype.storm.contrib.signals.StormSignalConnection;

/**
 * Generates {@link StormSignalConnection}s that close their connections after
 * receiving the first message.
 */
public class QuickCloseStormSignalConnectionFactory {
  /**
   * Closes the {@link StormSignalConnection} upon first signal.
   */
  private static class QuickCloseSignalListener implements SignalListener {
    private StormSignalConnection connection;
    private SignalListener listener;

    public QuickCloseSignalListener(SignalListener listener) {
      this.listener = listener;
    }

    public void setConnection(StormSignalConnection connection) {
      this.connection = connection;
    }

    @Override
    public void onSignal(byte[] data) {
      try {
        listener.onSignal(data);
      } finally {
        connection.close();
      }
    }
  }

  public StormSignalConnection createSignalConnection(String name, SignalListener listener) {
    QuickCloseSignalListener l = new QuickCloseSignalListener(listener);
    StormSignalConnection connection = new StormSignalConnection(name, l);
    // Must set connection so it can be closed
    l.setConnection(connection);
    return connection;
  }
}
