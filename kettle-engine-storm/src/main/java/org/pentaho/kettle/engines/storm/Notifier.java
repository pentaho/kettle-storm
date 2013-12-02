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
