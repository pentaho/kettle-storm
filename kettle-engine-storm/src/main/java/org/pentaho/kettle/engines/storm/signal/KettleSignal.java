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

package org.pentaho.kettle.engines.storm.signal;

import org.pentaho.kettle.engines.storm.KettleControlSignal;

import java.io.Serializable;

/**
 * Represents a control message for a Kettle step. This is used to indicate
 * state changes between steps running as Spouts or Bolts within a Storm
 * topology.
 * 
 * TODO Do we need the component and task ids here? Look into simply using the Tuple's.
 */
@SuppressWarnings("serial")
public class KettleSignal implements Serializable {
  private String componentId;
  private KettleControlSignal signal;
  private Integer taskId;

  public KettleSignal(String componentId, Integer taskId,
      KettleControlSignal signal) {
    this.componentId = componentId;
    this.taskId = taskId;
    this.signal = signal;
  }

  public String getComponentId() {
    return componentId;
  }

  public KettleControlSignal getSignal() {
    return signal;
  }

  @Override
  public String toString() {
    return "KettleSignal {componentId=" + componentId + ",taskId=" + taskId
        + ",signal=" + signal.name() + "}";
  }

  public Integer getTaskId() {
    return taskId;
  }
}
