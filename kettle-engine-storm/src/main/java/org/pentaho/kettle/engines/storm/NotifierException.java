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

/**
 * Indicates an error sending a notification.
 */
@SuppressWarnings("serial")
public class NotifierException extends Exception {

  public NotifierException(String message, Throwable cause) {
    super(message, cause);
  }

  public NotifierException(String message) {
    super(message);
  }

  public NotifierException(Throwable cause) {
    super(cause);
  }

}
