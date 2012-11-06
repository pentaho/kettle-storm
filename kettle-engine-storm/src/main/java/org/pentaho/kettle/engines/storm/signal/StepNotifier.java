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

import java.io.IOException;
import java.io.Serializable;

/**
 * This provides a mechanism for a step to signal has changed state. For
 * example, by sending a {@link KettleControlSignal#COMPLETE} a step can notify
 * other steps they can stop processing once they've exhausted all their input
 * from said step.
 */
public interface StepNotifier extends Serializable {
  /**
   * Signals a state change for a step.
   *
   * @param name   The name of the step that is "complete".
   * @param signal The control signal
   * @throws Exception An error was encountered while sending notification messages.
   */
  void notify(String name, KettleControlSignal signal) throws Exception;

  /**
   * Deserialize a kettle signal
   *
   * @param data
   * @return
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws ClassCastException     The deserialized class is not a KettleSignal
   */
  KettleSignal deserialize(byte[] data) throws IOException, ClassNotFoundException, ClassCastException;
}
