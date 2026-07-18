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

import backtype.storm.tuple.Values;

/**
 * A convenience class for making tuples of values with at most {@code N}
 * values.
 */
@SuppressWarnings("serial")
public class CappedValues extends Values {
  public CappedValues(int maxValues, Object... values) {
    if (maxValues < 1) {
      throw new IllegalArgumentException("max values must be > 0");
    }

    int max = Math.min(values.length, maxValues);
    for (int i = 0; i < max; i++) {
      add(values[i]);
    }
  }

}
