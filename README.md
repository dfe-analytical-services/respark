
# ReSpark

**Status:** Pre-release (0.0.x)

**ReSpark** is a Python library built on **PySpark** for generating privacy-preserving synthetic data from existing Spark DataFrames or schemas. It is designed to run in **any environment where Spark is available**, whether on a local machine, a cluster, or a cloud platform.

Modern data-driven solutions require realistic datasets for development, testing, and analytics. However, using production data introduces **privacy risks** and **governance challenges**. ReSpark provides a **privacy-first approach** to synthetic data generation, preserving the structure and statistical characteristics of your original data while minimising re-identification risk.

## Vision

- **Runs Anywhere Spark Runs**: Works in any environment where Spark DataFrames are processed, from local setups to large-scale clusters.
- **Privacy-First Design**: Includes validation reporting to check for residual sensitive information or re-identification risk.
- **Relational Integrity**: Maintains join consistency with appropriate handling of sensitive and non-sensitive fields.

## Installation

```bash
pip install respark
```

This package requires `pyspark` (Apache-2.0)

## Licence

© Crown Copyright 2025 Department for Education  
Licensed under the MIT Licence.

## Acknowledgements

Built on Apache Spark / PySpark (Apache License 2.0).
