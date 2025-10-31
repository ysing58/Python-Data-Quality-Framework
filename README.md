# PySpark-Data-Quality-Framework

## Overview
This repository contains a PySpark-based data quality validation framework for large-scale datasets. It demonstrates how Data Engineers can define, apply, and report data quality rules on Spark DataFrames.

## Features
- Rule-based validations on Spark DataFrames
- Built-in checks: not-null, uniqueness, ranges, regex, reference integrity
- Extensible rule API for custom business logic
- Summary metrics and failure sampling for debugging

## Project Structure
```
PySpark-Data-Quality-Framework/
├── README.md
└── data_quality_validator.py   # PySpark validation framework
```

## Usage
```bash
spark-submit data_quality_validator.py
```

## Skills Demonstrated
- PySpark DataFrame APIs
- Data quality and validation patterns
- Scalable ETL data checks
- Data Engineering best practices

## Author
Designed for Data Engineering/ETL portfolio demonstrations
