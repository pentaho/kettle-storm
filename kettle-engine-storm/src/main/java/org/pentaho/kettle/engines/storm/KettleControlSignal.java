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

/**
 * Control signals are sent from component to their dependencies to signal state
 * changes.
 */
public enum KettleControlSignal {
  /**
   * Indicates a component is done processing.
   */
  COMPLETE
}
