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

import java.util.List;

/**
 * The main API for emitting tuples from Kettle to Storm.
 */
public interface IKettleOutputCollector {
  List<Integer> emit(List<Object> tuple);
}
