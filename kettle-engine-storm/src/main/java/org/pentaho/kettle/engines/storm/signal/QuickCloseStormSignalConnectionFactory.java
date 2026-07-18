/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2002 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


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
