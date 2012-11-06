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

import backtype.storm.contrib.signals.bolt.BaseSignalBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.kettle.engines.storm.CappedValues;
import org.pentaho.kettle.engines.storm.KettleControlSignal;
import org.pentaho.kettle.engines.storm.KettleStormUtils;
import org.pentaho.kettle.engines.storm.signal.KettleSignal;
import org.pentaho.kettle.engines.storm.signal.StepNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

/**
 * A Kettle Step Bolt represents a Kettle step that receives input from at least one other Kettle step. This encapsulates the
 * logic required to receive input from Storm, process it, and emit any output from the step to be received by downstream bolts.
 */
@SuppressWarnings("serial")
public class KettleStepBolt extends BaseSignalBolt implements RowListener {
  private static final Logger logger = LoggerFactory
    .getLogger(KettleStepBolt.class);

  private KettleStormUtils utils = new KettleStormUtils();

  private String transXml;
  private String stepName;

  private transient StepMetaDataCombi step;
  private OutputCollector collector;

  private boolean done;

  private StepNotifier notifier;

  /**
   * A collection of tuples we've received. These are used to correlate output with input Tuples so message ack'ing properly groups output to the correct input.
   */
  private transient Deque<Tuple> receivedTuples;
  /**
   * The tuple we're currently processing. This is to correlate output with input Tuples so message ack'ing properly groups output to the correct input.
   */
  private transient Tuple currentTuple;

  public KettleStepBolt(String name, String transXml, StepMetaDataCombi step,
                        StepNotifier notifier) {
    super(name);
    if (step == null) {
      throw new IllegalArgumentException(
        "Step Meta required to create a new Kettle Step Bolt");
    }
    this.step = step;
    this.transXml = transXml;
    this.stepName = step.step.getStepname();
    this.notifier = notifier;
  }

  private StepMetaDataCombi getStep() {
    if (step == null) {
      try {
        step = utils.getStep(transXml, stepName);
      } catch (KettleException e) {
        throw new IllegalStateException(
          "Error processing transformation for bolt for step: "
            + stepName, e);
      }

      step.step.addRowListener(this);
    }
    return step;
  }

  @Override
  public void prepare(@SuppressWarnings("rawtypes") Map conf,
                      TopologyContext context, OutputCollector collector) {
    super.prepare(conf, context, collector);
    this.collector = collector;
    this.receivedTuples = new LinkedList<>();
  }

  @Override
  public void execute(Tuple input) {
    try {
      logger.debug("{} bolt received {}", stepName, input);
      // Cache the current tuple so we can anchor emitted values properly
      // This will not work for any step that batches records between calls to processRow()
      // TODO Make this work for all steps - we need a message id from Kettle to correlate tuple to message id.
      receivedTuples.addLast(input);
      injectRow(input);
    } catch (Exception ex) {
      throw new RuntimeException("Error converting tuple to Kettle row for step " + stepName,
        ex);
    }

    if (isInfoSource(input.getSourceComponent())) {
      // Immediately ack messages from info sources. We cannot determine how
      // they'll be used due to the lack of message identifiers in Kettle.
      // Assume these messages are ancillary to the input row sets messages.
      collector.ack(receivedTuples.removeLast());
    } else {
      processRows();
    }
  }

  private void injectRow(Tuple input) {
    RowSet rowSet = findRowSet(input.getSourceComponent());
    logger.debug("Injecting row to rowSet: {}", input.getSourceComponent());
    RowMetaInterface rowMeta = rowSet.getRowMeta();
    rowSet.putRow(rowMeta, utils.convertToRow(rowMeta, input.getValues().toArray()));
  }

  private RowSet findRowSet(String stepName) {
    // Look through info streams first
    for (StreamInterface infoStream : getStep().stepMeta.getStepMetaInterface().getStepIOMeta().getInfoStreams()) {
      if (stepName.equals(infoStream.getStepname())) {
        return getStep().step.getTrans().findRowSet(infoStream.getStepname(), 0, this.stepName, 0);
      }
    }
    for (RowSet rs : getStep().step.getInputRowSets()) {
      if (stepName.equals(rs.getOriginStepName())) {
        return rs;
      }
    }
    throw new IllegalArgumentException(String.format("Could not locate row set for a step with the name '%s'", stepName));
  }

  /**
   * Process a row for every received "input" (non-info) tuple.
   */
  private void processRows() {
    if (!isInfoInputComplete()) {
      logger.debug("Info is not complete - not processing rows yet!");
      // If we haven't received all rows for info streams do not call processRow as we'll block waiting for them. :(
      return;
    }
    logger.debug("Starting to process rows for {}. {} pending rows to process", stepName, receivedTuples.size());
    try {
      do {
        currentTuple = receivedTuples.peekFirst();
        logger.debug("Processing tuple: {}", currentTuple);
        try {
          // Keep track of how many rows we have before we start to process to
          // determine if processRow() actually consumed anything.
          long rowsRemaining = getPendingRowCount();
          logger.debug("pending row count: {}", rowsRemaining);
          done = !getStep().step.processRow(step.meta, step.data);
          logger.debug("pending row count after processRow: ", getPendingRowCount());
          if (getPendingRowCount() != rowsRemaining) {
            // Rows were consumed and ack
            receivedTuples.remove();
            collector.ack(currentTuple);
          }
        } catch (KettleException e) {
          if (currentTuple != null) {
            receivedTuples.remove();
            collector.fail(currentTuple);
          }
          throw new RuntimeException("Error processing a row for step "
            + stepName, e);
        }
      } while (!done && !receivedTuples.isEmpty());
    } finally {
      if (done) {
        try {
          getStep().step.batchComplete();
        } catch (KettleException ex) {
          logger.error("kettle exception completing batch for step " + stepName, ex);
        }
        getStep().step.dispose(step.meta, step.data);
        logger.debug("Step complete: {}", stepName);
        try {
          notifier.notify(stepName, KettleControlSignal.COMPLETE);
        } catch (Exception e) {
          logger.warn(stepName + ": Error notifying downstream steps", e);
        }
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    utils.declareOutputFields(step, declarer);
  }

  @Override
  public void errorRowWrittenEvent(RowMetaInterface rowMeta, Object[] row)
    throws KettleStepException {
  }

  @Override
  public void rowReadEvent(RowMetaInterface rowMeta, Object[] row)
    throws KettleStepException {
  }

  @Override
  public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] row)
    throws KettleStepException {
    CappedValues values = new CappedValues(rowMeta.getValueMetaList()
      .size(), row);
    if (!values.isEmpty()) {
      if (currentTuple == null) {
        // If the current tuple is null we've likely processed all received
        // tuples and are simply processing to get a state of "done". If any
        // rows are emited as part of that last dummy call to processRow this
        // will happen.
        StringBuilder sb = new StringBuilder();
        for (Object o : row) {
          sb.append(o).append(" ");
        }
        logger.warn("Current tuple unknown for new output on bolt (" + stepName + "): " + row + ": " + sb);
      }
      collector.emit(currentTuple, values);
    }
  }

  @Override
  public void onSignal(byte[] data) {
    Object o = data;
    try {
      o = notifier.deserialize(data);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    logger.debug("Signal received for step {}: {}", stepName, o);

    if (o instanceof KettleSignal) {
      KettleSignal signal = (KettleSignal) o;

      switch (signal.getSignal()) {
        case COMPLETE:
          // Assume only one input for now...
          logger.debug("Input is complete for bolt %s: %s\n", stepName, signal.getStepName());
          // Set the row set to "done"
          RowSet rowSet = findRowSet(signal.getStepName());
          rowSet.setDone();

          // If all row sets (info and input) are complete then this step is completely done!
          // We have to attempt to process a row for the step to realize it has nothing more to read.
          // If all row sets are not complete but info input is and we have
          // pending rows we should start to process them - we may have already
          // received all input.
          if (isInputComplete() || (isInfoInputComplete() && !receivedTuples.isEmpty())) {
            if (!done) {
              processRows();
            }
          } else {
            logger.debug("Input is not complete. Still waiting for rows...");
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported signal: " + signal.getSignal());
      }
    }
  }


  /**
   * Calculates how many rows are waiting to be processed on across all input row sets.
   *
   * @return The number of rows in all input row sets.
   */
  private long getPendingRowCount() {
    long pendingRowCount = 0L;
    // InputRowSets does not return info stream row sets until they are ready. Then it returns them until the rows are consumed.
    for (RowSet rs : getStep().step.getInputRowSets()) {
      if (!isInfoSource(rs.getOriginStepName())) {
        // Only include non-info row sets in this calculation since info rows will be fully consumed once the first row is processed.
        logger.debug(rs.getName() + ": " + rs.size());
        pendingRowCount += rs.size();
      }
    }
    return pendingRowCount;
  }

  /**
   * Determines if a given step name is connected to the step for this bolt via an info stream.

   * @param stepName The name of a step.
   * @return True if {@code stepName} is connected to the step for this bolt via an info stream.
   */
  private boolean isInfoSource(String stepName) {
    for (StreamInterface infoStream : getStep().stepMeta.getStepMetaInterface().getStepIOMeta().getInfoStreams()) {
      if (infoStream.getStepname().equals(stepName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determines if this bolt is waiting for more input from any info streams.
   *
   * @return True if this bolt is waiting for more input from an info stream.
   */
  private boolean isInfoInputComplete() {
    // Look through info streams first
    for (StreamInterface infoStream : getStep().stepMeta.getStepMetaInterface().getStepIOMeta().getInfoStreams()) {
      RowSet rs = getStep().step.getTrans().findRowSet(infoStream.getStepname(), 0, stepName, 0);
      if (!rs.isDone()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Determines if this bolt is waiting for any additional input.
   *
   * @return True if this bolt is expecting more input.
   */
  private boolean isInputComplete() {
    for (RowSet rs : getStep().step.getInputRowSets()) {
      if (!rs.isDone()) {
        return false;
      }
    }
    return isInfoInputComplete();
  }
}
