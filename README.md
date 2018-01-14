# Trumania

## Documentation and tutorial

Trumania is a scenario-based random dataset generator library in python 3. In order to use it, a scenario that describes the dataset
to create has to be written, in python 3, and then executed. 

See the [wiki](https://github.com/RealImpactAnalytics/trumania/wiki) in this
repo for a detailed documentation of each of the features. The wiki also
contains a step-by-step explanation of 4 example scenarios.

You can also have a look at the example scenario in the [tests/](tests/) folder of this repository.

## How to install 

Trumania is not packaged in any special way, the way it is used at the moment is simply to clone the code and install the required dependencies. This section describes how to do that.

Make sure you have python 3 and pip installed. Then make sure pipenv is installed:

```sh
# make sure you're using pip from a python 3 installation 
pip3 install --user pipenv
```


then install all python dependencies for this project: 

```sh
pipenv install --three
```

The steps below mention to prefix the commands with `pipenv run` whenever necessary in order to have access to those python dependencies. Alternatively, you can enter the corresponding virtualenv once with `pipenv shell`, in which case that prefix is no longer necessary. See [https://docs.pipenv.org](https://docs.pipenv.org) for more details about how to use pipenv to handle python dependencies. 


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

## Contributing

This section provides a few pointers on how to handle the trumania codebase.

### Running Trumania unit tests locally


```sh
# make sure you are not inside another pipenv shell when running this
pipenv run py.test -s -v
```

### Python linting
Run `pipenv run flake8`. If nothing is returned, the correct styling has been applied.
