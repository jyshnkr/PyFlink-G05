## This is Nandini Kandi. 
# MinimalPageRankBeam

Java Quickstart for Apache Beam

<https://beam.apache.org/get-started/quickstart-java>

## Set up Environment

- Java
- Maven

## Get the Sample Project

```PowerShell
mvn archetype:generate `
 -D archetypeGroupId=org.apache.beam `
 -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
 -D archetypeVersion=2.36.0 `
 -D groupId=edu.nwmsu.bigdata `
 -D artifactId=page-rank-sec02-grp05 `
 -D version="0.1" `
 -D package=edu.nwmissouri.groupOfFive.nandinikandi `
 -D interactiveMode=false`
```

## Execute PR Quick Start

```PowerShell
mvn compile exec:java -D exec.mainClass=edu.nwmissouri.groupOfFive.nandinikandi.MinimalPageRankKandi
```

### Link to My WIKI page
https://github.com/jyshnkr/PyFlink-G05/wiki/Nandini-Kandi 

### Link to My Group's repo
https://github.com/jyshnkr/PyFlink-G05 

### Link to MyCode folder
https://github.com/jyshnkr/PyFlink-G05/tree/main/Nandini

### Link to my minimalPageRank.java
https://github.com/jyshnkr/PyFlink-G05/blob/main/Nandini/src/main/java/edu/nwmissouri/groupOfFive/nandinikandi/MinimalPageRankKandi.java 

### Link to my RankedPage.java
https://github.com/jyshnkr/PyFlink-G05/blob/main/Nandini/src/main/java/edu/nwmissouri/groupOfFive/nandinikandi/RankedPage.java 

### Link to my VotingPage.java
https://github.com/jyshnkr/PyFlink-G05/blob/main/Nandini/src/main/java/edu/nwmissouri/groupOfFive/nandinikandi/VotingPage.java  

### Work done by me
I have implemented Job1Finalizer, Job2Mapper and Job2Updater classes. And added kandiMapper1(), runJob2Iteration(), deleteFiles() and main() methods to MinimalPageRankKandi. And got the expected output.
