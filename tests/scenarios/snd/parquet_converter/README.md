# Data Module Template
As its name indicates, this data module presents an example how a data module looks like. If you are new to data module and want to develop your own, it is good to start your journey with this template.
 
## Pre-requisites
### Technology Stacks
See [Dataflow Engine](https://github.com/RealImpactAnalytics/dataflow-engine/).
### Data Module Specification, Convention, Optimization, etc
See [Data module confluence space](https://realimpactanalytics.atlassian.net/wiki/display/BIG/Data+Modules).


## Quick Start for Development
### Manifest File
A data module has a manifest (manifest.yaml at the root folder) to tell the world who it is, what it does, when to run, what its parameters are and how to run it.

* Who it is is defined through it id, name, description, version, etc.
* What it does is defined through what it produces in the output field.
* When to run it is defined by its dependencies (only when these datasources are available and/or only after these data modules have run)
* How to run it is defined by the (business and technical) parameters you should call it with.
    
### Data Module Definition
A concrete data module must define a class that inherits `DataModule` class. 
It also needs to define two objects, that will serve as entry points:
- one needs to be called `Execute`
- the other one needs to be called `Visualize`
`ExampleDataModule` is a good example of how to implement such a class and 2 objects.


### Data Module White-box Test
In order to facilitate the validation process for a data module, you are supposed to write one more several test suites. See `ExampleDataModuleSpec`.

### Data Module (Black-box) Test
A good data module must also contain (black-box) test scenarios. 

* Write a small class that create data module instances from parameters. See `ExampleDataModuleTest` 
* Prepare test scenarios locally stored at `$DATA_MODULE_FOLDER/sandbox/data` folder. An example can be found [on S3](https://console.aws.amazon.com/s3/home?region=eu-west-1#&bucket=datamodule-test&prefix=template/0/). In order to see if the tests pass execute `sbt it:test`.
* Upload test scenarios to S3 and the command to upload to S3 looks like `aws s3 cp toS3/scenario_01 s3://datamodule-test/template/0/scenario_01 --recursive`
* The data module test can be executed by `sbt module-test`. The system first download data from S3 then execute all the test scenarios one by one.
  
[More information on data module test](https://realimpactanalytics.atlassian.net/wiki/display/BIG/Data+Module+Test).





