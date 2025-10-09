# Job Market Analysis

A Scala project for analyzing job market data using Apache Spark.

## Prerequisites

Make sure you have the following installed:

- [Java JDK 21](https://adoptium.net/) (or compatible version)
- [Scala](https://www.scala-lang.org/download/)
- [sbt](https://www.scala-sbt.org/)
- [VS Code](https://code.visualstudio.com/) with [Metals extension](https://scalameta.org/metals/)

> Note: Scala and sbt versions are configured in `build.sbt`.


## Project Setup

1. Clone the repository:

```bash
git clone https://github.com/Kajlid/job-market-analysis.git
cd job-market-analysis
```

2. Set up dependencies
Fetch dependencies and set up the project with sbt:
```bash
sbt reload
sbt update
```

3. Run the project:
```bash
sbt run
```
This will compile the Scala code and run the main application