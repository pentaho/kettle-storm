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