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

import org.pentaho.di.core.exception.KettleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KettleStorm {
  private static final Logger logger = LoggerFactory.getLogger(KettleStorm.class);

  public static void main(String[] args) throws Exception {
    if (args == null || args.length != 1) {
      throw new IllegalArgumentException("Must specify transformation file name");
    }

    StormExecutionEngineConfig config = new StormExecutionEngineConfig();
    config.setDebugMode(Boolean.valueOf(System.getProperty("kettle-storm-debug", "false")));
    config.setLocalMode(Boolean.valueOf(System.getProperty("kettle-storm-local-mode", "false")));
    config.setTopologyJar(System.getProperty("kettle-storm-topology-jar", StormExecutionEngineConfig.loadStormTopologyJarFromConfiguration()));
    config.setTransformationFile(args[0]);

    final StormExecutionEngine engine = new StormExecutionEngine(config);

    if (config.isLocalMode()) {
      logger.debug("Executing in local mode");
    }

    engine.init();
    engine.execute();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        logger.info("Stopping transformation");
        try {
          engine.stop();
        } catch (KettleException ex) {
          logger.error("Error stopping topology for Kettle transformation", ex);
        }
      }
    });

    logger.info("Waiting for transformation to complete...");
    logger.info("Press CTRL-C to kill the topology and exit.");

    try {
      do {
        // Wait until the transformation is complete
      } while (!engine.isComplete(100, TimeUnit.MILLISECONDS));
      logger.debug("Transformation complete!");
    } finally {
      engine.stop();
    }
  }

}
