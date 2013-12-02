package org.pentaho.kettle.engines.storm;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

public interface KettleTopologyBuilder {
	/**
	 * Build a topology capable of executing the provided transformation.
	 * 
	 * @param conf
	 *            Storm configuration to use to configure connection
	 *            information.
	 * @param trans
	 *            Transformation meta to build topology from.
	 * @return Storm topology capable of executing the Kettle transformation.
	 * @throws KettleException
	 *             Error loading the transformation details or initializing the
	 *             kettle environment
	 */
	StormTopology build(Config config, TransMeta trans) throws KettleException;
}
