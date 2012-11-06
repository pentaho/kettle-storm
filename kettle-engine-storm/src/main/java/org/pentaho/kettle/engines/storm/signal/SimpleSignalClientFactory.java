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

import backtype.storm.contrib.signals.client.SignalClient;

/**
 * Creates {@link SignalClient}s for a known ZooKeeper instance.
 */
@SuppressWarnings("serial")
public class SimpleSignalClientFactory implements SignalClientFactory {

  private String zkConnectionString;

  /**
   * Create a new factory that creates clients that use the provided ZooKeeper
   * connection string.
   *
   * @param zkConnectionString ZooKeeper connection string for clients to use when establishing
   *                           their connections
   */
  public SimpleSignalClientFactory(String zkConnectionString) {
    this.zkConnectionString = zkConnectionString;
  }

  @Override
  public SignalClient createClient(String name) {
    return new SignalClient(zkConnectionString, name);
  }

}
