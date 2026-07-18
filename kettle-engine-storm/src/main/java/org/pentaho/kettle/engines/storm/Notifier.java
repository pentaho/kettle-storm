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

import java.io.Serializable;
import java.util.Map;

/**
 * This provides a mechanism for signaling a state change. For example, by
 * sending a {@link KettleControlSignal#COMPLETE} a transformation can notify
 * interested parties it has completed.
 */
public interface Notifier extends Serializable {
  /**
   * Initialize this notifier.
   * 
   * @param stormConf
   *          The Storm configuration for this notifier.
   */
  @SuppressWarnings("rawtypes")
  void init(Map stormConf);

  /**
   * Signals a state change.
   * 
   * @param id
   *          The identifier sending the message.
   * @param signal
   *          The control signal.
   * @throws Exception
   *           An error was encountered while sending notification messages.
   */
  void notify(String id, KettleControlSignal signal) throws NotifierException;

  /**
   * Called when the component utilizing this notifier is being cleaned up.
   * There is no guarantee that cleanup will be called.
   */
  void cleanup();
}
