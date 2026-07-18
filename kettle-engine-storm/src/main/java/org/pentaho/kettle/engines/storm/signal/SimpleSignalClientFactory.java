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
