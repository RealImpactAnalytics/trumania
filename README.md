# lab-data-generator

Some folders are stored on S3:

`datagenerator/components/_DB` is on `s3://lab-data-generator-db`

`datagenerator/components/geographies/source_data` is on `s3://lab-data-generator-geographies`

You can download them with aws-cli:

```
> pip install awscli
> mkdir datagenerator/components/_DB
> cd datagenerator/components/_DB
# make sure you have your AWS env variables set
> aws s3 cp s3://lab-data-generator-db . --recursive
```

# Documentation + Tutorial

See https://realimpactanalytics.atlassian.net/wiki/display/LM/Data+generator+tutorial

# Scenarios

You will find the S&D Scenario here in `tests/scenarios/snd`
