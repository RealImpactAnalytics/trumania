## Structure

This folder contains different elements:

* `circus/` -> the actual generator scenario
* `parquet_converter/` -> used to convert the csv outputs of the generator to parquet files
that can be used by the S&D product.
* `generate_mobile_seed.py` -> used to convert the csv outputs of the generator to a zip archive
that can be used as a mobile sync seed.

## All the steps to create an S&D seed

```
cd circus
python main_1_build.py
python main_2_run.py
python main_3_target.py
cd ..
cd parquet_converter
sbt run # not sure
cd ..
python generate_mobile_seed.py
```
