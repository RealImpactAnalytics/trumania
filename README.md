# Trumania

## Documentation and tutorial

See the [wiki](https://github.com/RealImpactAnalytics/trumania/wiki) in this repo for the detailed documentation of the generator. 

You can also have a look at the S&D example Scenario here in `tests/scenarios/snd`.

## How to install 

Trumania is not (yet) packaged in any special way, the way it is used at the moment is simply to have the code, the required dependencies, and execute the necessary python script directly. 

Make sure you have python 2.7 and pip installed.

Then, if pipenv is not yet present on your laptop, install it: 

```sh
# make sure you're using pip from a python 3 installation 
pip3 install --user pipenv
```

Otherwise, make sure you have the latest version:

```sh
pipenv update
```

then install all dependencies for this project: 
```sh
pipenv install --three
```

See [https://docs.pipenv.org](https://docs.pipenv.org) for more details about how to use pipenv to handle python dependencies.


## Where and how to create a scenario

To create a scenario, simply create another python project that depends on trumania: 

```sh
mkdir -p /path/to/your/project
cd /path/to/your/project

# then simply add a dependency towards the location where you downloaded trumania:
pipenv install -e /path/to/trumania/
```

You can then create your scenario in python, let's call it `burbanks_and_friends_talking.py`.  In order to execute it, simply launch it from pipenv: 

```sh
pipenv run python burbanks_and_friends_talking.py  
```

## Running Trumania unit tests locally


```sh
# make sure you are not inside another pipenv shell when running this
pipenv run py.test -s 
```

## Python linting
Run `pipenv shell` and then `flake8`. If nothing is returned, the correct styling has been applied.

## Test data
Some folders are stored on S3:

`trumania/components/_DB` is on `s3://lab-data-generator-db`

`trumania/components/geographies/source_data` is on `s3://lab-data-generator-geographies`

You can download them with aws-cli:

```sh
pip install awscli
mkdir trumania/components/_DB
cd trumania/components/_DB
# make sure you have your AWS env variables set
aws s3 cp s3://lab-data-generator-db . --recursive
```

