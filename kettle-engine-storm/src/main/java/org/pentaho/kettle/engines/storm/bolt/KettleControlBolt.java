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

package org.pentaho.kettle.engines.storm.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pentaho.kettle.engines.storm.KettleControlSignal;
import org.pentaho.kettle.engines.storm.Notifier;
import org.pentaho.kettle.engines.storm.StormExecutionEngine;
import org.pentaho.kettle.engines.storm.signal.KettleSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * This bolt aggregates all the final {@link KettleSignal}s from leaf bolts and
 * notifies {@link StormExecutionEngine} that the transformation has completed.
 */
@SuppressWarnings("serial")
public class KettleControlBolt extends BaseRichBolt {
  private static final Logger logger = LoggerFactory
      .getLogger(KettleControlBolt.class);

  private String transformationName;
  private Notifier notifier;
  private OutputCollector collector;
  private Set<String> leafSteps;
  private Map<String, List<Integer>> componentToPendingTasks;

  /**
   * Create a new control bolt to check for completion of the given steps.
   * 
   * @param name
   *          Name of this bolt. Used only for the ZooKeeper connection.
   * @param topologyName
   *          The name of the topology this bolt is participating in. This is
   *          the name of the resource it will signal when it has received a
   *          complete signal from all leaf steps.
   * @param leafSteps
   *          List of all leaf steps that must complete before the
   *          transformation is to be considered complete.
   */
  public KettleControlBolt(String transformationName, Notifier notifier,
      Set<String> leafSteps) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(transformationName));
    Preconditions.checkNotNull(leafSteps);
    Preconditions.checkArgument(!leafSteps.isEmpty(),
        "At least 1 leaf step is expected");
    this.transformationName = transformationName;
    this.notifier = notifier;
    this.leafSteps = leafSteps;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    this.collector = collector;
    // Build the map of tasks that must complete for the transformation to have
    // completed
    componentToPendingTasks = new HashMap<String, List<Integer>>();
    for (String componentId : leafSteps) {
      List<Integer> tasks = context.getComponentTasks(componentId);
      if (tasks == null || tasks.isEmpty()) {
        throw new IllegalStateException("No tasks defined for leaf step " + componentId);
      }
      componentToPendingTasks.put(componentId,
          new ArrayList<Integer>(tasks));
    }
    notifier.init(stormConf);
  }

  @Override
  public void execute(Tuple input) {
    // We only ever expect signals to be routed to us.
    try {
      KettleSignal signal = (KettleSignal) input.getValue(0);

      logger.info("Received signal from " + signal.getComponentId() + ": "
          + signal.getSignal());

      // Remove the pending task from the component's list
      List<Integer> pendingTaskIds = componentToPendingTasks.get(signal
          .getComponentId());
      if (pendingTaskIds == null || !pendingTaskIds.remove(signal.getTaskId())) {
        // TODO How can we fail the topology if this happens?
        throw new IllegalStateException(
            "Unexpected completion message received: componentId="
                + signal.getComponentId() + ",taskId=" + signal.getTaskId()
                + ".");
      }
      if (pendingTaskIds.isEmpty()) {
        componentToPendingTasks.remove(signal.getComponentId());
      }
      if (componentToPendingTasks.isEmpty()) {
        logger
            .info("All leaf steps have completed. Sending transformation complete message.");
        // Transformation is complete! Fire the signal.
        notifier.notify(transformationName, /* not used */
            KettleControlSignal.COMPLETE);
      }
      collector.ack(input);
    } catch (Exception ex) {
      logger.error("Error processing tuple: " + input, ex);
      collector.fail(input);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // We don't output anything.
  }

  @Override
  public void cleanup() {
    super.cleanup();
    notifier.cleanup();
  }
}
