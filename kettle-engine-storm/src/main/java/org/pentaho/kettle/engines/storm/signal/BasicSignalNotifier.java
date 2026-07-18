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

import java.util.Map;

import org.pentaho.kettle.engines.storm.KettleControlSignal;
import org.pentaho.kettle.engines.storm.Notifier;
import org.pentaho.kettle.engines.storm.NotifierException;

import backtype.storm.contrib.signals.SignalListener;
import backtype.storm.contrib.signals.StormSignalConnection;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * A notifier that uses ZooKeeper via Storm Signals to send notifications. This
 * notifier will ignore the specific signal provided to
 * {@link #notify(String, KettleControlSignal)} and instead always send an empty
 * message.
 */
@SuppressWarnings("serial")
public class BasicSignalNotifier implements Notifier {

  private String id;
  private StormSignalConnection signalConnection;

  public BasicSignalNotifier(String name) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name),
        "name cannot be null or empty");
    this.id = name;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void init(Map stormConf) {
    // TODO Refactor this to use ZooKeeper directly
    signalConnection = new StormSignalConnection(id, new SignalListener() {
      @Override
      public void onSignal(byte[] data) {
        throw new IllegalStateException(
            "not expecting any signals to be sent to " + id);
      }
    });
    try {
      signalConnection.init(stormConf);
    } catch (Exception ex) {
      throw new RuntimeException("Error creating signal connection", ex);
    }
  }

  /**
   * Send a simple empty message to the component with the given id.
   * 
   * @param id
   *          Component to notify.
   * @param signal
   *          Not used.
   */
  @Override
  public void notify(String id, KettleControlSignal signal)
      throws NotifierException {
    // Note: Signal value does not matter. The reception of any message
    // indicates transformation is complete. This is received by the
    // StormExecutionEngine.
    try {
      signalConnection.send(id, new byte[0]);
    } catch (Exception ex) {
      throw new NotifierException("Error notifying " + id + " with signal "
          + signal, ex);
    }
  }

  @Override
  public void cleanup() {
    signalConnection.close();
  }
}
