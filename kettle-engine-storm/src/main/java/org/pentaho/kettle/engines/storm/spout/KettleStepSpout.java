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

import backtype.storm.contrib.signals.spout.BaseSignalSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.kettle.engines.storm.BaseSpoutOutputCollector;
import org.pentaho.kettle.engines.storm.CollectorRowListener;
import org.pentaho.kettle.engines.storm.KettleControlSignal;
import org.pentaho.kettle.engines.storm.KettleStormUtils;
import org.pentaho.kettle.engines.storm.signal.StepNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Kettle Step Spout represents a Kettle step that produces records and specifically does not receive any input from other Kettle steps.
 * This encapsulates the logic to produce messages within Storm to be processed by downstream bolts.
 */
@SuppressWarnings("serial")
public class KettleStepSpout extends BaseSignalSpout {
  private static final Logger logger = LoggerFactory
    .getLogger(KettleStepSpout.class);
  private KettleStormUtils utils = new KettleStormUtils();

  private String transXml;
  private String stepName;

  private transient StepMetaDataCombi step;

  private boolean done = false;

  private StepNotifier notifier;
  /**
   * The set of pending messages we're waiting to be ack'd. This should be thread-safe.
   */
  private Set<Object> pendingMessages;

  public KettleStepSpout(String name, String transXml,
                         StepMetaDataCombi step, StepNotifier notifier) {
    super(name);
    if (transXml == null || step == null) {
      throw new NullPointerException();
    }
    this.stepName = name;
    this.step = step;
    this.transXml = transXml;
    this.notifier = notifier;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {
    super.open(conf, context, collector);
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
  }

  @Override
  public void ack(Object msgId) {
    handleCompleted(msgId);
  }

  @Override
  public void fail(Object msgId) {
    logger.error("Message failed processing: " + msgId);
    handleCompleted(msgId);
  }

  private void handleCompleted(Object msgId) {
    // Message fully processed - remove it from our list
    if (!pendingMessages.remove(msgId)) {
      throw new IllegalStateException("Unexpected message id ack'd: " + msgId);
    }
    if (done && pendingMessages.isEmpty()) {
      step.step.dispose(step.meta, step.data);
      step.step.markStop();
      try {
        notifier.notify(stepName, KettleControlSignal.COMPLETE);
      } catch (Exception e) {
        logger.warn(stepName + ": Error notifying downstream steps", e);
      }
    }
  }

  @Override
  public void onSignal(byte[] data) {
    logger.debug("Spout ({}) received signal: {}", stepName, data);
    try {
      notifier.deserialize(data);
    } catch (Exception ex) {
      logger.error("Unable to process signal notification", ex);
    }
  }
}
