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

import java.io.IOException;
import java.util.Properties;

/**
 * Defines configuration and runtime settings for the
 * {@link StormExecutionEngine}.
 */
public class StormExecutionEngineConfig {
  /**
   * The jar to submit along with the topology. This should include everything Kettle needs to boot up and then load plugins from elsewhere.
   * By default, it will use the *-with-dependencies.jar generated with Maven from this project. See README.md for more information.
   */
  private String topologyJar;
  private String transformationFile;
  private boolean debugMode;
  private boolean localMode;

  public String getTopologyJar() {
    return topologyJar;
  }

  public void setTopologyJar(String topologyJar) {
    this.topologyJar = topologyJar;
  }

  public String getTransformationFile() {
    return transformationFile;
  }

  public void setTransformationFile(String transformationFile) {
    this.transformationFile = transformationFile;
  }

  public boolean isDebugMode() {
    return debugMode;
  }

  public void setDebugMode(boolean debugMode) {
    this.debugMode = debugMode;
  }

  public boolean isLocalMode() {
    return localMode;
  }

  public void setLocalMode(boolean localMode) {
    this.localMode = localMode;
  }

  public static String loadStormTopologyJarFromConfiguration() throws IOException {
    Properties p = new Properties();
    p.load(StormExecutionEngineConfig.class.getResourceAsStream("/kettle-storm.properties"));
    return p.getProperty("kettle.topology.jar");
  }
}
