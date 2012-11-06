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

import backtype.storm.contrib.signals.client.SignalClient;
import com.google.common.base.Preconditions;
import org.pentaho.kettle.engines.storm.KettleControlSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

/**
 * Send signals to steps
 */
@SuppressWarnings("serial")
public class StormSignalsStepNotifier implements StepNotifier {
  private static final Logger logger = LoggerFactory
      .getLogger(StormSignalsStepNotifier.class);
  private SignalClientFactory clientFactory;
  private List<String> dependentNodes;

  public StormSignalsStepNotifier(SignalClientFactory clientFactory, List<String> dependentNodes) {
    Preconditions.checkNotNull(clientFactory);
    Preconditions.checkNotNull(dependentNodes);
    Preconditions.checkArgument(!dependentNodes.isEmpty(), "At least one dependent node is required to create a valid notifier!");
    this.dependentNodes = dependentNodes;
    this.clientFactory = clientFactory;
  }

  @Override
  public void notify(String name, KettleControlSignal controlSignal) throws Exception {

    KettleSignal signal = new KettleSignal(name, controlSignal);

    // Overkill but clients are only used once - for now.
    // Create and destroy clients as needed
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream os = new ObjectOutputStream(baos);

    logger.info(String.format("Node %s is done. Notifying: %s", name, dependentNodes));

    for (String node : dependentNodes) {
      SignalClient client = clientFactory.createClient(node);
      logger.info(String.format("Notifying client for node '%s': '%s' is %s", node, name, controlSignal.name()));
      client.start();
      try {
        os.writeObject(signal);
        os.flush();
        client.send(baos.toByteArray());
        os.reset();
      } finally {
        client.close();
      }
    }
  }

  @Override
  public KettleSignal deserialize(byte[] data) throws IOException, ClassNotFoundException, ClassCastException {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream is = new ObjectInputStream(bais);
    return KettleSignal.class.cast(is.readObject());
  }
}
