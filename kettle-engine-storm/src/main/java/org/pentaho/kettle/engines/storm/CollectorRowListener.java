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

import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepMetaDataCombi;

/**
 * Listens for rows emitted from Kettle steps and passes them to an {@link IKettleOutputCollector} so they may be routed by Storm.
 */
public class CollectorRowListener implements RowListener {

  private KettleStormUtils utils = new KettleStormUtils();

  private IKettleOutputCollector collector;
  private int numFields = 0;

  public CollectorRowListener(StepMetaDataCombi step, IKettleOutputCollector collector, int numFields) {
    if (step == null || collector == null) {
      throw new NullPointerException();
    }
    if (numFields < 1) {
      throw new IllegalArgumentException("numFields must be > 0");
    }
    this.collector = collector;
    this.numFields = numFields;
  }

  @Override
  public void errorRowWrittenEvent(RowMetaInterface rowMeta, Object[] out) throws KettleStepException {
  }

  @Override
  public void rowReadEvent(RowMetaInterface rowMeta, Object[] out) throws KettleStepException {
  }

  @Override
  public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] out) throws KettleStepException {
    collector.emit(new CappedValues(numFields, utils.convertToRow(rowMeta, out)));
  }

}