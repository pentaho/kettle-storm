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

import backtype.storm.spout.SpoutOutputCollector;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Wraps an {@link SpoutOutputCollector} so pending messages may be tracked. A {@link org.pentaho.kettle.engines.storm.spout.KettleStepSpout}
 * relies on this to know when all the data it has emitted has been fully processed.
 */
public class BaseSpoutOutputCollector implements IKettleOutputCollector {
  private SpoutOutputCollector out;
  /**
   * The collection to add message ids to when emiting tuples. This should be
   * thread-safe.
   */
  private Set<Object> pendingMessageIds;

  public BaseSpoutOutputCollector(SpoutOutputCollector out, Set<Object> pendingMessageIds) {
    if (out == null) {
      throw new NullPointerException("output collector must not be null");
    }
    if (pendingMessageIds == null) {
      throw new NullPointerException("pending messages set must not be null");
    }
    this.out = out;
    this.pendingMessageIds = pendingMessageIds;
  }

  @Override
  public List<Integer> emit(List<Object> tuple) {
    // Generate a message Id so these tuples can be properly ACK'd when they've
    // been processed. We use message acknowledging to determine when all output
    // from a Spout has been processed.
    Object messageId = UUID.randomUUID();
    List<Integer> taskIds = out.emit(tuple, messageId);
    pendingMessageIds.add(messageId);
    return taskIds;
  }

}
