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

package org.pentaho.kettle.engines.storm.spout;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.kettle.engines.storm.BaseSpoutOutputCollector;
import org.pentaho.kettle.engines.storm.CollectorRowListener;
import org.pentaho.kettle.engines.storm.KettleControlSignal;
import org.pentaho.kettle.engines.storm.KettleStormUtils;
import org.pentaho.kettle.engines.storm.signal.KettleSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

/**
 * A Kettle Step Spout represents a Kettle step that produces records and specifically does not receive any input from other Kettle steps.
 * This encapsulates the logic to produce messages within Storm to be processed by downstream bolts.
 */
@SuppressWarnings("serial")
public class KettleStepSpout extends BaseRichSpout {
  private static final Logger logger = LoggerFactory
    .getLogger(KettleStepSpout.class);
  private KettleStormUtils utils = new KettleStormUtils();

  private String componentId;
  private Integer taskId;

  private String transXml;
  private String stepName;

  private transient StepMetaDataCombi step;

  private boolean done = false;
  
  private Object signalCompleteMessageId;

  /**
   * The set of pending messages we're waiting to be ack'd. This should be thread-safe.
   */
  private Set<Object> pendingMessages;
  
  private SpoutOutputCollector collector;

  public KettleStepSpout(String name, String transXml,
                         StepMetaDataCombi step) {
    if (transXml == null || step == null) {
      throw new NullPointerException();
    }
    this.stepName = name;
    this.step = step;
    this.transXml = transXml;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {
    componentId = context.getThisComponentId();
    taskId = context.getThisTaskId();
    this.collector = collector;
    try {
      this.step = utils.getStep(transXml, stepName);
    } catch (KettleException e) {
      throw new IllegalStateException(
        "Error processing transformation for spout for step: "
          + stepName, e);
    }

    if (this.step == null) {
      throw new IllegalStateException(
        "Step could not be found for spout: " + stepName);
    }

    pendingMessages = Collections.newSetFromMap(new ConcurrentHashMap<Object, Boolean>(1000));

    step.step.addRowListener(new CollectorRowListener(step,
      new BaseSpoutOutputCollector(collector, pendingMessages), utils.getOutputFields(
      step).size()));
  }

  @Override
  public void nextTuple() {
    if (!done) {
      try {
        done = !step.step.processRow(step.meta, step.data);
      } catch (KettleException e) {
        throw new RuntimeException("Error processing a row for step "
          + step.step.getStepname(), e);
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    utils.declareOutputFields(step, declarer);
    declarer.declareStream("signal", new Fields("signal"));
  }

  @Override
  public void ack(Object msgId) {
    // Only handle completed row messages. If the ack'd message id is the signal
    // complete message then we're done!
    if (!msgId.equals(signalCompleteMessageId)) {
      handleCompleted(msgId);
    }
  }

  @Override
  public void fail(Object msgId) {
    if (msgId.equals(signalCompleteMessageId)) {
      logger.error("Error processing signal complete message. Resending...");
      // Send the signal complete message again
      // TODO we should set a retry limit
      signalComplete();
    } else {
      logger.error("Message failed processing: " + msgId);
      handleCompleted(msgId);
    }
  }

  private void handleCompleted(Object msgId) {
    // Message fully processed - remove it from our list
    if (!pendingMessages.remove(msgId)) {
      throw new IllegalStateException("Unexpected message id ack'd: " + msgId);
    }
    if (done && pendingMessages.isEmpty()) {
      step.step.dispose(step.meta, step.data);
      step.step.markStop();
      signalComplete();
    }
  }

  private void signalComplete() {
    logger.info("Signaling complete for step " + stepName + " with taskId=" + taskId + ".");
    try {
      signalCompleteMessageId = UUID.randomUUID();
      collector.emit("signal", Collections.<Object> singletonList(new KettleSignal(componentId, taskId, KettleControlSignal.COMPLETE)), signalCompleteMessageId);
    } catch (Exception e) {
      logger.warn(stepName + ": Error notifying downstream steps", e);
    }
  }
}
