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

package org.pentaho.kettle.engines.storm;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.kettle.engines.storm.bolt.KettleControlBolt;
import org.pentaho.kettle.engines.storm.bolt.KettleStepBolt;
import org.pentaho.kettle.engines.storm.signal.BasicSignalNotifier;
import org.pentaho.kettle.engines.storm.spout.KettleStepSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * A collection of utility methods for working with Kettle and Storm.
 *
 * TODO refactor this into more meaningful components
 */
@SuppressWarnings("serial")
public class KettleStormUtils implements Serializable {
  private static final Logger logger = LoggerFactory
    .getLogger(KettleStormUtils.class);

  private static final String KETTLE_TOPOLOGY_NAME = "kettle.topology.name";

  /**
   * Create a topology from a transformation.
   *
   * @param conf Storm configuration to use to configure connection information.
   * @param meta Transformation meta to build topology from.
   * @return Storm topology capable of executing the Kettle transformation.
   * @throws KettleException Error loading the transformation details or initializing the kettle environment
   * @throws IOException     Error generating the transformation XML from the meta.
   */
  public StormTopology createTopology(Config conf, TransMeta meta) throws KettleException, IOException {
    initKettleEnvironment();
    TransConfiguration transConfig = new TransConfiguration(meta,
      new TransExecutionConfiguration());
    String transXml = transConfig.getXML();
    Trans trans = new Trans(meta);
    trans.prepareExecution(null);
    List<StepMetaDataCombi> steps = trans.getSteps();

    String topologyName = generateTopologyName(meta.getName());
    setTopologyName(conf, topologyName);

    TopologyBuilder builder = new TopologyBuilder();

    Set<String> leafSteps = collectLeafStepNames(trans);
    
    String controlBoltId = topologyName + "-control-bolt";
    BasicSignalNotifier notifier = new BasicSignalNotifier(controlBoltId);
    BoltDeclarer controlBoltDeclarer = builder.setBolt(controlBoltId, new KettleControlBolt(topologyName, notifier, leafSteps));
    for (StepMetaDataCombi step : steps) {
      step.step.init(step.meta, step.data);

      // The control bolt must receive all signal tuples from all leaf steps
      if (leafSteps.contains(step.step.getStepname())) {
        controlBoltDeclarer.allGrouping(step.step.getStepname(), "signal");
      }

      if (isSpout(step)) {
        builder.setSpout(step.step.getStepname(), new KettleStepSpout(
          step.step.getStepname(), transXml, step), step.step.getStepMeta().getCopies())
          .setMaxTaskParallelism(step.step.getStepMeta().getCopies());
      } else {
        BoltDeclarer bd = builder.setBolt(step.step.getStepname(),
          new KettleStepBolt(step.step.getStepname(), transXml,
            step), step.step.getStepMeta().getCopies())
          .setMaxTaskParallelism(step.step.getStepMeta().getCopies());
        for (StreamInterface info : step.stepMeta.getStepMetaInterface().getStepIOMeta().getInfoStreams()) {
          StepMetaDataCombi infoStep = findStep(trans,
            info.getStepname());
          bd.fieldsGrouping(info.getStepname(), getOutputFields(infoStep));
          bd.allGrouping(info.getStepname(), "signal");
        }
        for (RowSet input : step.step.getInputRowSets()) {
          StepMetaDataCombi inputStep = findStep(trans,
            input.getOriginStepName());
          bd.fieldsGrouping(input.getOriginStepName(),
            getOutputFields(inputStep));
          // All bolts must receive all signal tuples from all previous steps
          bd.allGrouping(input.getOriginStepName(), "signal");
        }
      }
    }

    return builder.createTopology();
  }

  /**
   * Find all steps that do not have output hops.
   * 
   * @param trans
   *          The transformation.
   * @return The set of all steps that do not have output hops.
   */
  private Set<String> collectLeafStepNames(Trans trans) {
    Set<String> leafSteps = new HashSet<String>();
    for (StepMetaDataCombi step : trans.getSteps()) {
      if (isLeafStep(trans, step)) {
        leafSteps.add(step.step.getStepname());
      }
    }
    return leafSteps;
  }
  
  private boolean isLeafStep(Trans trans, StepMetaDataCombi step) {
    return trans.getTransMeta().findNextSteps(step.stepMeta).isEmpty();
  }

  /**
   * Finds a step by name within a transformation.
   * 
   * @param trans
   *          Transformation to search within.
   * @param stepName
   *          Name of step to look up.
   * @return The first step found whose stepname matches the provided one.
   */
  private StepMetaDataCombi findStep(Trans trans, String stepName) {
    for (StepMetaDataCombi step : trans.getSteps()) {
      if (stepName.equals(step.step.getStepname())) {
        return step;
      }
    }
    throw new RuntimeException("Unable to find step with name " + stepName);
  }

  /**
   * Determines if the step should be converted to a Spout. A step should be
   * converted to a spout if it receives no input.
   *
   * @param step
   * @return
   */
  private boolean isSpout(StepMetaDataCombi step) {
    return step.step.getInputRowSets().isEmpty();
  }

  public void declareOutputFields(StepMetaDataCombi step,
                                  OutputFieldsDeclarer declarer) {
    declarer.declare(getOutputFields(step));
  }

  /**
   * Determine the output row meta for this step.
   *
   * @param step Step to determine output rows for.
   * @return The output row meta for the step provided.
   */
  private RowMetaInterface getOutputRowMeta(StepMetaDataCombi step) {
    try {
      return step.step.getTrans().getTransMeta()
        .getStepFields(step.step.getStepMeta());
    } catch (KettleException ex) {
      throw new RuntimeException("Unable to get output fields from step "
        + step.step.getStepname());
    }
  }

  /**
   * Returns the fields a step produces as output.
   *
   * @param step Step to determine output fields for.
   * @return The field layout the step will produce.
   */
  public Fields getOutputFields(StepMetaDataCombi step) {
    String[] fieldNames = getOutputRowMeta(step).getFieldNames();
    String[] outputFieldNames = new String[fieldNames.length];
    for (int i = 0; i < fieldNames.length; i ++) {
      outputFieldNames[i] = step.step.getStepname() + "-" + fieldNames[i];
    }
    return new Fields(outputFieldNames);
  }

  /**
   * Initialize the Kettle environment.
   *
   * @throws KettleException If an error is encountered during initialization
   */
  public void initKettleEnvironment() throws KettleException {
    if (!KettleEnvironment.isInitialized()) {
      logger.debug("Initializing Kettle Environment...");
      logger.debug("Kettle Home: " + Const.getKettleDirectory());
      KettleEnvironment.init();
    }
  }

  public StepMetaDataCombi getStep(String transXml, String stepName) throws KettleException {
    initKettleEnvironment();
    TransConfiguration transConfiguration = TransConfiguration
      .fromXML(transXml);
    TransMeta transMeta = transConfiguration.getTransMeta();
    Trans trans = new Trans(transMeta);
    trans.prepareExecution(null);
    transMeta.setUsingThreadPriorityManagment(false);
    trans.setRunning(true); // GO GO GO
    for (StepMetaDataCombi step : trans.getSteps()) {
      if (stepName.equals(step.step.getStepname())) {
        if (!step.step.init(step.meta, step.data)) {
          throw new RuntimeException("Unable to initialize step "
            + step.step.getStepname());
        }
        for (RowSet rowSet : step.step.getInputRowSets()) {
          rowSet.setRowMeta(getOutputRowMeta(findStep(trans,
            rowSet.getOriginStepName())));
        }
        return step;
      }
    }
    throw new RuntimeException("Unable to locate step: " + stepName);
  }

  /**
   * Convert a row from Kettle object to Java object.
   *
   * @param rowMeta Meta information about the row provided.
   * @param tuple   Row of data to convert.
   * @return Converted values based on the row meta given.
   */
  public Object[] convertToRow(RowMetaInterface rowMeta, Object[] tuple) {
    for (int i = 0; i < tuple.length; i++) {
      try {
        if (tuple[i] != null) {
          ValueMetaInterface meta = rowMeta.getValueMeta(i);
          switch (meta.getType()) {
            case ValueMetaInterface.TYPE_STRING:
              tuple[i] = meta.getString(tuple[i]);
              break;
            case ValueMetaInterface.TYPE_NUMBER:
              tuple[i] = meta.getNumber(tuple[i]);
              break;
            case ValueMetaInterface.TYPE_INTEGER:
              tuple[i] = meta.getInteger(tuple[i]);
              break;
            case ValueMetaInterface.TYPE_DATE:
              tuple[i] = meta.getDate(tuple[i]);
              break;
            default:
              throw new IllegalArgumentException(
                "Unsupported data type: "
                  + rowMeta.getValueMeta(i).getTypeDesc());
          }
        }
      } catch (Exception ex) {
        throw new RuntimeException("unable to convert value: "
          + tuple[i], ex);
      }
    }

    return tuple;
  }

  /**
   * Generate a unique topology name.
   *
   * @param name Prefix for the topology name so its easily identifiable.
   * @return A unique topology name, prefixed with the name provided.
   */
  private String generateTopologyName(String name) {
    return name + "-" + UUID.randomUUID().toString();
  }

  /**
   * Set the topology name in a configuration so it can be retrieved by another
   * process later.
   *
   * @param conf Configuration to store topology name in.
   * @param name Topology name to set.
   */
  private void setTopologyName(Config conf, String name) {
    conf.put(KETTLE_TOPOLOGY_NAME, name);
  }

  /**
   * Retrieve the topology name from a Storm configuration.
   *
   * @param conf Storm configuration used to create the topology from a Kettle
   *             transformation.
   * @return The name of the topology created for a Kettle transformation with
   *         the provided configuration.
   */
  public String getTopologyName(Config conf) {
    return (String) conf.get(KETTLE_TOPOLOGY_NAME);
  }
}
