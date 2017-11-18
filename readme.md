# Flink Samples

Refer to 

https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/datastream_api.html


Create flink project using maven

https://ci.apache.org/projects/flink/flink-docs-release-1.3/quickstart/java_api_quickstart.html

```shell
$ mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-java      \
      -DarchetypeVersion=1.3.2
```

Add folder as src/test/java ; right click java -> Mark Directory as -> Test Sources roots
Add folder as src/test/resources ; right click resources -> Mark Directory as -> Test Resources roots


# Based on Sample

```java
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
```

The environment to run the job is different for batch and stream.