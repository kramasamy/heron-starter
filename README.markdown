# Heron Starter

Learn to use Heron!

---

Table of Contents

* <a href="#getting-started">Getting started</a>
* <a href="#maven">Using heron-starter with Maven</a>
* <a href="#intellij-idea">Using heron-starter with IntelliJ IDEA</a>

---

<a name="getting-started"></a>

# Getting started

## Prerequisites

First, you will need a laptop that connect to internet using Wi-FI. In the laptop,
make sure that you have `java` and `git` installed and in your user's `PATH`.  

Second, you will need a Twitter account. If you do not have a Twitter account, please 
create one. From your Twitter account, you can follow the Heron experts:

    @karthikz, @louis_fumaosong, @billgraham

Third, if you need to use Twitter firehose/Tweet stream for your idea, 
create a set of credentials by following the instructions at 

    https://dev.twitter.com/discussions/631

Fourth (optional), partner with someone to form a group. We encourage 2/3 students per group. 

Finally, make sure you have the heron-starter code available on your machine.  Git/GitHub beginners may want to use the following command to download the latest heron-starter code and change to the new directory that contains the downloaded code.

    $ git clone https://github.com/kramasamy/heron-starter.git && cd heron-starter


## Overview

heron-starter contains a variety of examples of using Heron.  If this is your first time working with Heron, check out these topologies first:

1. [ExclamationTopology](src/jvm/heron/starter/ExclamationTopology.java):  Basic topology written in all Java
2. [WordCountTopology](src/jvm/heron/starter/WordCountTopology.java):  Basic topology for counting words all written in Java

After you have familiarized yourself with these topologies, take a look at the other topopologies in
[src/jvm/heron/starter/](src/jvm/heron/starter/) such as [RollingTopWords](src/jvm/heron/starter/RollingTopWords.java)
for more advanced implementations.

If you want to learn more about how Heron works, please head over to the
[Heron project page](http://heronstreaming.io).

<a name="maven"></a>

# Using heron-starter with Maven

## Install Maven

[Maven](http://maven.apache.org/) is an alternative to Leiningen.  Install Maven (preferably version 3.x) by following
the [Maven installation instructions](http://maven.apache.org/download.cgi).

## Packaging heron-starter for use on a Aurora/Mesos cluster

You can package a jar suitable for submitting to a Aurora/Mesos cluster with the command:

    $ mvn pom.xml package

This will package your code and all the non-Heron dependencies into a single "uberjar" at the path
`target/heron-starter-{version}-jar-with-dependencies.jar`.

## Submitting your jobs Locally

After compiling your package, you can submit your job locally by following the [Quick Start Guide](http://twitter.github.io/heron/docs/getting-started/)

    $ heron submit local \
      ./target/heron-starter-{version}-jar-with-dependencies.jar \ # The path of the topology's jar file
      heron.starter.WordCountTopology \ # The topology's Java class
      WordCountTopology \ # The name of the topology
      --deploy-deactivated # Deploy in deactivated mode

This will submit the topology to your locally running Heron cluster but it won’t activate the topology.  To activate, use the following Heron cli commands:

    $ heron activate local WordCountTopology
    $ heron deactivate local WordCountTopology
    $ heron kill local WordCountTopology

## Running unit tests

Use the following Maven command to run the unit tests that ship with heron-starter.  

    $ mvn pom.xml test


<a name="intellij-idea"></a>

# Using heron-starter with IntelliJ IDEA

## Importing heron-starter as a project in IDEA

The following instructions will import heron-starter as a new project in IntelliJ IDEA.

* Open _File > Import Project..._ and navigate to the top-level directory of your heron-starter clone (e.g.
  `~/${YOUR-PATH}/heron-starter`).
* Select _Import project from external model_, select "Maven", and click _Next_.
* In the following screen, enable the checkbox _Import Maven projects automatically_.  Leave all other values at their
  defaults.  Click _Next_.
* Click _Next_ on the following screen about selecting Maven projects to import.
* Select the JDK to be used by IDEA for heron-starter, then click _Next_.
    * At the time of this writing you should use JDK 6.
    * It is strongly recommended to use Sun/Oracle JDK 7 rather than OpenJDK 7.
* You may now optionally change the name of the project in IDEA.  The default name suggested by IDEA is "heron-starter".
  Click _Finish_ once you are done.
