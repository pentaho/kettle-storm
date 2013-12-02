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
