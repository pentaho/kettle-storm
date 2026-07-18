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

import java.io.Serializable;

/**
 * Factory for constructing Signal Clients.
 */
public interface SignalClientFactory extends Serializable {
  /**
   * Create a client that listens for messages addressed to {@code name}.
   *
   * @param name Name of the client.
   * @return A ready to use Signal client.
   */
  SignalClient createClient(String name);
}
