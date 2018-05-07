# Cypher for Apache Spark example project

This is a minimal example project that uses Cypher on Apache Spark.

# Key Dependencies

Please note in the pom.xml the usage of the two repositories, which are needed to 
obtain Morpheus and CAPS dependencies.

```
    <repository>
      <id>opencypher-public</id>
      <name>neo-snapshots</name>
      <url>https://neo.jfrog.io/neo/opencypher-public</url>
      <snapshots>
        <enabled>true</enabled>
      </snapshots>
    </repository>

    <repository>
      <id>morpheus-releases</id>
      <name>Morpheus private release repository</name>
      <url>https://neo.jfrog.io/neo/morpheus-release</url>
    </repository>
```

Additionally, the user's $HOME/.m2/settings.xml should look like this:
(substituting your beta access username/password)

```
 <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                          https://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
        <id>opencypher-private</id>
        <username>bdg-training</username>
        <password>PASSWORD</password>
    </server>
    <server>
        <id>opencypher-public</id>
        <username>bdg-training</username>
        <password>PASSWORD</password>
    </server>
    <server>
        <id>morpheus-releases</id>
        <username>neo4j-beta-access</username>
        <password>PASSWORD</password>
    </server>
  </servers>
</settings>
```