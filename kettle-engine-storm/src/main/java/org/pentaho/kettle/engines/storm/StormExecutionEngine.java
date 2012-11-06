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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.signals.SignalListener;
import backtype.storm.contrib.signals.StormSignalConnection;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.thrift7.TException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.kettle.engines.storm.signal.QuickCloseStormSignalConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * An engine capable of processing data as defined by a Kettle transformation as a Storm topology. It provides a simple mechanism for
 * starting, polling for status, and stopping a Storm topology.
 */
public class StormExecutionEngine {
  private static final Logger logger = LoggerFactory.getLogger(StormExecutionEngine.class);
  private static KettleStormUtils util = new KettleStormUtils();
  private QuickCloseStormSignalConnectionFactory signalConnectionFactory = new QuickCloseStormSignalConnectionFactory();

  private LocalCluster localCluster = null;

  private StormExecutionEngineConfig config;

  private TransMeta meta;

  private Config stormConfig;

  // Flag to indicate the engine is executing
  private volatile boolean running = false;
  // This is used to synchronize blocking for the transformation to complete
  private CountDownLatch transCompleteLatch;
  private String topologyName;

  public StormExecutionEngine(StormExecutionEngineConfig config) {
    if (config == null) {
      throw new NullPointerException("config must not be null");
    }
    this.config = config;
  }

  /**
   * Prepare the engine to execute the transformation located at
   * {@link StormExecutionEngineConfig#getTransformationFile()}.
   *
   * @throws KettleException Error loading transformation
   */
  public void init() throws KettleException {
    stormConfig = loadStormConfig();
    util.initKettleEnvironment();
    meta = new TransMeta(config.getTransformationFile());
    setJarToUpload(config.getTopologyJar());
  }

  /**
   * Execute the transformation as a Storm topology.
   *
   * @throws IOException          Error generating the transformation XML from the meta.
   * @throws KettleException      Error reading transformation settings or starting execution entirely.
   * @throws InterruptedException Thread was interrupted while waiting for the topology to
   *                              complete. {@link #stop()} should be called before propagating.
   * @throws Exception            Generic exception was thrown while establishing a connection to
   *                              ZooKeeper.
   */
  public synchronized void execute() throws KettleException, IOException, InterruptedException {
    StormTopology topology = util.createTopology(stormConfig, meta);

    topologyName = util.getTopologyName(stormConfig);

    transCompleteLatch = new CountDownLatch(1);
    // TODO Support more than one end step. Deserialize message and check for specific steps completing instead of just counting them.
    final StormSignalConnection signalConnection = signalConnectionFactory.createSignalConnection(topologyName, new SignalListener() {
      @Override
      public void onSignal(byte[] data) {
        // If anything is received for the topology name we consider it to mean the transformation is complete
        logger.info("Received transformation complete message");
        transCompleteLatch.countDown();
      }
    });

    submitTopology(topologyName, stormConfig, topology);
    logger.info(String.format("Submitted transformation as topology '%s'\n", topologyName));
    running = true;
    try {
      signalConnection.init(stormConfig);
    } catch (Exception ex) {
      try {
        stop();
      } catch (KettleException e) {
        logger.warn("Error stopping topology after signal connection failure", e);
      }
      throw new KettleException("Unable to establish signal connection to ZooKeeper.", ex);
    }
  }

  /**
   * Return the topology name that was started as a result of executing this
   * engine.
   *
   * @return The topology name used to execute the transformation provided to
   *         this engine, or null if the engine has not been started.
   */
  public String getTopologyName() {
    return topologyName;
  }

  /**
   * A blocking call to determine if the transformation done executing.
   *
   * @param timeout the maximum time to wait
   * @param unit    the time unit of the timeout argument
   * @return True if the topology this engine executed is complete
   * @throws InterruptedException  If the current thread is interrupted while waiting
   * @throws IllegalStateException if the engine has not been started
   */
  public boolean isComplete(long timeout, TimeUnit unit) throws InterruptedException {
    if (!running) {
      throw new IllegalStateException("Engine not started");
    }
    return transCompleteLatch.await(timeout, unit);
  }

  /**
   * Stop the running transformation's topology in Storm.
   *
   * @throws KettleException If an error was encountered stopping the Storm topology.
   */
  public synchronized void stop() throws KettleException {
    if (!running) {
      // Not running, nothing to do here
      return;
    }

    try {
      logger.debug("Attempting to kill topology: " + topologyName);
      killTopology(stormConfig, topologyName);
      logger.debug("Topology killed successfully");
      running = false;
    } catch (Exception ex) {
      throw new KettleException("Unable to kill topology: " + topologyName, ex);
    }
  }

  /**
   * Load the Storm {@link Config} by reading command line options and the Storm
   * config files.
   *
   * @return Configuration with all possible configurations loaded from the
   *         environment.
   */
  @SuppressWarnings("unchecked")
  private Config loadStormConfig() {
    final Config conf = new Config();
    conf.setDebug(config.isDebugMode());
    conf.putAll(Utils.readCommandLineOpts());
    conf.putAll(Utils.readStormConfig());

    if (config.isLocalMode()) {
      conf.put(Config.STORM_CLUSTER_MODE, "local");
      conf.put(Config.STORM_ZOOKEEPER_SERVERS, Collections.singletonList("localhost"));
      conf.put(Config.STORM_ZOOKEEPER_PORT, 2000);
    }

    return conf;
  }

  /**
   * Storm needs to know what jar contains code to execute a topology. It keys
   * off the "storm.jar" System property. We will set it if its not already set
   * to the provided jar path.
   *
   * @param jarPath Path to jar file to submit with topology. This should be a jar
   *                containing all required resources to execute the transformation.
   *                Plugins need not be included if they can be resolved from
   *                $KETTLE_HOME/plugins.
   */
  private static void setJarToUpload(String jarPath) {
    String stormJar = System.getProperty("storm.jar", jarPath);
    System.setProperty("storm.jar", jarPath);
    logger.debug("Configured Storm topology jar as: {}", stormJar);
  }

  @SuppressWarnings("rawtypes")
  private void submitTopology(String name, Map stormConf, StormTopology topology) throws KettleException {
    if (config.isLocalMode()) {
      localCluster = new LocalCluster();
      localCluster.submitTopology(name, stormConf, topology);
    } else {
      try {
        StormSubmitter.submitTopology(name, stormConf, topology);
      } catch (Exception ex) {
        throw new KettleException("Error submitting topology " + name, ex);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private void killTopology(Map conf, String name) throws NotAliveException, TException {
    if (config.isLocalMode()) {
      localCluster.killTopology(name);
      localCluster.shutdown();
    } else {
      NimbusClient client = NimbusClient.getConfiguredClient(conf);
      client.getClient().killTopology(name);
    }
  }

}
