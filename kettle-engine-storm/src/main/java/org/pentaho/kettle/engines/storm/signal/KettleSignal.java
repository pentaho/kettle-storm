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
