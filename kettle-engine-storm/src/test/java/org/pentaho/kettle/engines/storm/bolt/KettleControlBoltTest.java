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

package org.pentaho.kettle.engines.storm.bolt;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.kettle.engines.storm.KettleControlSignal;
import org.pentaho.kettle.engines.storm.Notifier;
import org.pentaho.kettle.engines.storm.signal.KettleSignal;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class KettleControlBoltTest {
  private static final String TRANS_NAME = "transformation 1";
  private static final String STEP_1 = "step 1";
  private static final String STEP_2 = "step 2";
  private static final int TASK_ID_1 = 1723;
  private static final int TASK_ID_2 = 18;
  private static final KettleSignal STEP_1_COMPLETE = new KettleSignal(STEP_1,
      TASK_ID_1, KettleControlSignal.COMPLETE);
  private static final KettleSignal STEP_2_COMPLETE = new KettleSignal(STEP_2,
      TASK_ID_2, KettleControlSignal.COMPLETE);

  private IMocksControl control;
  private Notifier notifier;
  private TopologyContext context;
  private OutputCollector collector;

  @SuppressWarnings("rawtypes")
  @Before
  public void init() {
    control = EasyMock.createControl();
    notifier = control.createMock(Notifier.class);
    notifier.init(EasyMock.<Map> anyObject());
    EasyMock.expectLastCall().anyTimes();
    collector = control.createMock(OutputCollector.class);
    context = control.createMock(TopologyContext.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void construct() {
    new KettleControlBolt(null, notifier, Collections.singleton("step"));
  }

  @Test(expected = IllegalStateException.class)
  public void prepare_no_tasks_for_leaf_step_null() {
    KettleControlBolt bolt = new KettleControlBolt(TRANS_NAME, notifier,
        Collections.singleton(STEP_1));
    EasyMock.expect(context.getComponentTasks(STEP_1)).andReturn(null);

    control.replay();
    bolt.prepare(Collections.emptyMap(), context, collector);
  }

  @Test(expected = IllegalStateException.class)
  public void prepare_no_tasks_for_leaf_step_empty() {
    KettleControlBolt bolt = new KettleControlBolt(TRANS_NAME, notifier,
        Collections.singleton(STEP_1));
    EasyMock.expect(context.getComponentTasks(STEP_1)).andReturn(
        Collections.<Integer> emptyList());

    control.replay();
    bolt.prepare(Collections.emptyMap(), context, collector);
  }

  /**
   * Create a tuple for the given signal.
   * 
   * @param signal
   *          Signal to emit as a single value tuple.
   * @return The tuple.
   */
  private Tuple createTupleForSignal(KettleSignal signal) {
    Tuple input = control.createMock(Tuple.class);
    EasyMock.expect(input.getValue(0)).andReturn(signal).anyTimes();
    return input;
  }

  /**
   * Verify the last task to complete triggers the notifier.
   */
  @Test
  public void execute_last_task() throws Exception {
    // Test set up
    KettleControlBolt bolt = new KettleControlBolt(TRANS_NAME, notifier,
        Collections.singleton(STEP_1));
    List<Integer> taskIds = Collections.singletonList(TASK_ID_1);
    EasyMock.expect(context.getComponentTasks(STEP_1)).andReturn(taskIds);
    Tuple step1Complete = createTupleForSignal(STEP_1_COMPLETE);

    // Expect that our notifier is notified after receiving a complete signal
    // for our one and only leaf node
    notifier.notify(TRANS_NAME, KettleControlSignal.COMPLETE);
    EasyMock.expectLastCall();

    // The tuple should be acknowledged
    collector.ack(step1Complete);
    EasyMock.expectLastCall();

    control.replay();
    bolt.prepare(Collections.emptyMap(), context, collector);
    bolt.execute(step1Complete);
    control.verify();
  }

  /**
   * Verify notifications are not sent if there are pending steps.
   */
  @Test
  public void execute_not_last_step() throws Exception {
    // Test set up
    KettleControlBolt bolt = new KettleControlBolt(TRANS_NAME, notifier,
        Sets.newHashSet(STEP_1, STEP_2));
    EasyMock.expect(context.getComponentTasks(STEP_1)).andReturn(
        Collections.singletonList(TASK_ID_1));
    EasyMock.expect(context.getComponentTasks(STEP_2)).andReturn(
        Collections.singletonList(TASK_ID_2));
    Tuple step1Complete = createTupleForSignal(STEP_1_COMPLETE);

    // The tuple should be acknowledged
    collector.ack(step1Complete);
    EasyMock.expectLastCall();

    control.replay();
    bolt.prepare(Collections.emptyMap(), context, collector);
    bolt.execute(step1Complete);
    control.verify();
  }

  /**
   * Verify notifications are sent after all leaf steps are complete.
   */
  @Test
  public void execute_multiple_steps() throws Exception {
    // Test set up
    KettleControlBolt bolt = new KettleControlBolt(TRANS_NAME, notifier,
        Sets.newHashSet(STEP_1, STEP_2));
    EasyMock.expect(context.getComponentTasks(STEP_1)).andReturn(
        Collections.singletonList(TASK_ID_1));
    EasyMock.expect(context.getComponentTasks(STEP_2)).andReturn(
        Collections.singletonList(TASK_ID_2));
    Tuple step1Complete = createTupleForSignal(STEP_1_COMPLETE);
    Tuple step2Complete = createTupleForSignal(STEP_2_COMPLETE);

    // The tuples should be acknowledged
    collector.ack(step1Complete);
    EasyMock.expectLastCall();
    collector.ack(step2Complete);
    EasyMock.expectLastCall();

    // Expect that our notifier is notified after receiving a complete signal
    // for our one and only leaf node
    notifier.notify(TRANS_NAME, KettleControlSignal.COMPLETE);
    EasyMock.expectLastCall();

    control.replay();
    bolt.prepare(Collections.emptyMap(), context, collector);
    bolt.execute(step1Complete);
    bolt.execute(step2Complete);
    control.verify();
  }

  /**
   * Verify notifications are sent after all copies of the leaf steps have
   * completed.
   */
  @Test
  public void execute_single_leaf_step_with_multiple_copies() throws Exception {
    // Test set up
    KettleControlBolt bolt = new KettleControlBolt(TRANS_NAME, notifier,
        Sets.newHashSet(STEP_1));
    EasyMock.expect(context.getComponentTasks(STEP_1)).andReturn(
        Lists.newArrayList(TASK_ID_1, TASK_ID_2));
    Tuple task1Complete = createTupleForSignal(new KettleSignal(STEP_1,
        TASK_ID_1, KettleControlSignal.COMPLETE));
    Tuple task2Complete = createTupleForSignal(new KettleSignal(STEP_1,
        TASK_ID_2, KettleControlSignal.COMPLETE));

    // The tuples should be acknowledged
    collector.ack(task1Complete);
    EasyMock.expectLastCall();
    collector.ack(task2Complete);
    EasyMock.expectLastCall();

    // Expect that our notifier is notified after receiving a complete signal
    // for our one and only leaf node
    notifier.notify(TRANS_NAME, KettleControlSignal.COMPLETE);
    EasyMock.expectLastCall();

    control.replay();
    bolt.prepare(Collections.emptyMap(), context, collector);
    bolt.execute(task1Complete);
    bolt.execute(task2Complete);
    control.verify();
  }

  /**
   * Verify receiving a signal for a non-leaf step is a failure case.
   */
  @Test
  public void execute_unexpected_signal() throws Exception {
    // Test set up
    KettleControlBolt bolt = new KettleControlBolt(TRANS_NAME, notifier,
        Sets.newHashSet(STEP_1));
    EasyMock.expect(context.getComponentTasks(STEP_1)).andReturn(
        Collections.singletonList(TASK_ID_1));
    Tuple unexpectedSignalTuple = createTupleForSignal(new KettleSignal(
        "unknown step", 1, KettleControlSignal.COMPLETE));

    // The tuple should be failed since we're not expected it.
    collector.fail(unexpectedSignalTuple);
    EasyMock.expectLastCall();

    control.replay();
    bolt.prepare(Collections.emptyMap(), context, collector);
    bolt.execute(unexpectedSignalTuple);
    control.verify();
  }

  @Test
  public void cleanup() {
    KettleControlBolt bolt = new KettleControlBolt(TRANS_NAME, notifier,
        Collections.singleton(STEP_1));

    notifier.cleanup();
    EasyMock.expectLastCall();

    control.replay();
    bolt.cleanup();
    control.verify();
  }
}
