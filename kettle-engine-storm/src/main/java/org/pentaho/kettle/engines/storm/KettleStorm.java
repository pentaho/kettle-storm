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
