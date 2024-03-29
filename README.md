Lasher
==========
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.soundvibe/lasher/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.soundvibe/lasher)
[![Build Status](https://travis-ci.org/soundvibe/lasher.svg)](https://travis-ci.org/soundvibe/lasher)
[![codecov](https://codecov.io/gh/soundvibe/lasher/branch/master/graph/badge.svg)](https://codecov.io/gh/soundvibe/lasher)

Lasher is an embeddable key-value store written in Java.

What is Lasher?
-------------------
Lasher is very lightweight embeddable persistent key-value store with very fast performance.

It is possible to store millions of elements in Lasher and use very little memory because all the data is persisted into memory mapped files.

Lasher could be used instead of any regular in-memory hashmap without sacrificing performance.
It is even faster than `ConcurrentHasMap<K,V>` in our benchmarks and uses much less memory.

Lasher stores consist of 2 binary files - index and data.

LasherDB
-------------------
General purpose key value store, partitioned by local shards.

LasherMap
-------------------
LasherMap implements `ConcurrentMap<K,V>` for easier interoperability with java maps.

TimeLash
-------------------
TimeLash is a time-series map backed by Lasher where data is partitioned by time intervals.
It supports very efficient data retention strategies.
To put or get values from the map, timestamp should be additionally provided, e.g.:

```java
try (var timeLash = new TimeLash<>(
    dir, //db directory
    Duration.ofHours(6), //data retention duration
    Serdes.STRING, //key serde
    Serdes.STRING)) // value serde {
    timeLash.put("foo", "bar", Instant.now());
    var bar = timeLash.get("foo", Instant.now());
}
```

Artifacts
-----------

Lasher is available on Maven Central, hence just add the following dependency:
```xml
<dependency>
    <groupId>net.soundvibe</groupId>
    <artifactId>lasher</artifactId>
    <version>0.0.3</version>
</dependency>
```

Scala SBT
```scala
libraryDependencies += "net.soundvibe" % "lasher" % "0.0.3"
```

Contributions
-----------

Any helpful feedback is more than welcome. This includes feature requests, bug reports, pull requests, constructive feedback, etc.

Copyright & License
-------------------

Lasher © 2020 Linas Naginionis. Licensed under the terms of the Apache License, Version 2.0.
