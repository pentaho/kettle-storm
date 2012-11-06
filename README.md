kettle-storm
============
An experimental execution environment to execute a Kettle transformation as a Storm topology.

How to use
----------
To run a local in-process Storm cluster for testing:
```
mvn compile exec:java -Dexec.args=src/main/resources/test.ktr -Dkettle-storm-local-mode=true
```

To execute against a running cluster on the same host:
```
mvn assembly:assembly -Passembly
mvn exec:java -Dexec.args=src/main/resources/test.ktr
```

Documentation
=============
Kettle Storm is an experimental execution environment to execute a Kettle transformation across a Storm cluster. This decomposes a transformation into a topology and wraps all steps in either a Storm Spout or a Bolt. The topology is then submitted to the cluster and is automatically killed once the transformation has finished processing all data.

Many things are not implemented. I've only tested this for the transformation files included on a small cluster. There are quite a few details left to be implemented. Some of which include:

- Steps that do not emit at least one message for every input:
  - Sampling
  - Aggregation
  - Sorting
  - Filtering
- First-class Spoon support
- Repository-based transformations
- Error handling
- Conditional hops
- Multiple end steps
- Sub-transformations
- Metrics: Kettle timing, throughput, logging

Executing a Kettle transformation with Storm
--------------------------------------------

### From a checkout
A Kettle transformation can be submitted as a topology using the included KettleStorm command-line application. To invoke it from Maven simply use the maven exec target with the Kettle transformation you wish to execute:
```
mvn assembly:assembly -Passembly
mvn exec:java -Dexec.args=src/main/resources/test.ktr
```

### From a release archive
Extract the release archive and run:
```
java -jar kettle-storm-${version}.jar path/to/my.ktr
```

### Configuration via System Properties

If additional options are required they can be provided as System Properties vai the command line in the format: `-Dargument=value`.

They are all optional and will be translated into ```StormExecutionEnvironmentConfig``` properties:

* ```kettle-storm-local-mode```: Flag indicating if you wish to execute the transformation as a Storm topology on an in-memory "local cluster" or remotely to an external Storm cluster. Defaults to ```false```.
* ```kettle-storm-debug```: Flag indicating you wish to enable debug messaging from Storm for the submitted topology. Defaults to ```false```.
* ```kettle-storm-topology-jar```: The path to the jar file to submit with the Storm topology. This is only required if you have created a custom jar with additional classes you wish to make available to the Kettle transformation without having to manually install plugins or configure the environment of each Storm host.

#### Storm Configuration

By default, Kettle Storm will submit topologies to a nimbus host running on localhost with the default connection settings included with Storm. If you'd like to use a specific storm.yaml file declare a System property on the command line:
```
mvn exec:java -Dstorm.conf.file=/path/to/storm.yaml -Dexec.args=src/main/resources/test.ktr
```

Storm configuration properties can be overriden by specifying them on the command line in the format:
```
-Dstorm.options=nimbus.host=my-nimbus,nimbus.thrift.port=6628
```

Embedding
---------
The Kettle execution engine that can submit topologies can be embedded in a Java application using ```StormExecutionEngine``` and ```StormExecutionEngineConfig```.

```StormExecutionEngine``` provides convenience methods for integrating within multithreaded environments:

- ```StormExecutionEngine.isComplete```: Blocks for the provided duration and returns ```true``` if the topology has completed successfully.
- ```StormExecutionEngine.stop```: Kills the topology running the transformation if it's still execution.

### Example Code

```
StormExecutionEngineConfig config = new StormExecutionEngineConfig();
config.setTransformationFile("/path/to/my.ktr");
StormExecutionEngine engine = new StormExecutionEngine(config);
engine.init();
engine.execute();
engine.isComplete(10, TimeUnit.MINUTE); // Block for up to 10 minutes while the topology executes.
```

Building a release archive
--------------------------
This is not complete; however, for now you must package the jar with all dependencies and assemble the topology jar. You must build the jars in this order.

```
mvn clean assembly:assembly -Passembly
mvn package
```

Once the jars are generated you can manually archive these artifacts in ```target/```:
```
kettle-engine-storm-0.0.1-SNAPSHOT-jar-with-dependencies.jar
kettle-engine-storm-0.0.1-SNAPSHOT.jar
```

External References
===================
Kettle: http://kettle.pentaho.com  
Storm: http://storm-project.net
