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
