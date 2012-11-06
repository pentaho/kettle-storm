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

import java.io.FileNotFoundException;
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
