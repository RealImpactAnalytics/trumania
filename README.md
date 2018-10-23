# Trumania

## Documentation and tutorial

Trumania is a scenario-based random dataset generator library in python 3. 

A [detailed step-by-step tutorial has is available on Datacamp](https://www.datacamp.com/community/tutorials/generate-data-trumania). 

The [Trumania github page](http://realimpactanalytics.github.io/trumania/) also contains 
a detailed documentation of each of the concepts as well as a step-by-step explanation of 4 example scenarios. Those scenarios, and more, are present in the [examples/](examples/) folder in this repository.

The code pydoc documentation is available [here](http://realimpactanalytics.github.io/trumania/py-modindex.html).

You can also join the Trumania slack channel: [trumania.slack.com](https://trumania.slack.com)

## How to install 

Trumania is not packaged in any special way, the way it is used at the moment is simply to clone the code and install the required dependencies. This section describes how to do that.

Pre-requisites: 

- Trumania only works with python 3.6 (TODO: allow 3.7: the current issue is a dependency on pandas 0.22)
- If you installed python 3 with homebrew, then the executable is called `python3` and pip is called `pip3`. See [homebrew python documentation](https://docs.brew.sh/Homebrew-and-Python.html) for details
- If you installed python 3 with Conda, make sure you understand how environments work since they might end up conflicting with pipenv environments. See [this ticket](https://github.com/pypa/pipenv/issues/699) for a discussion
- In anycase, in order to specify the exact path of the python to be used, you can always specify `--python /path/to/python` among the `pipenv` arguments. 

That being said, start by installing `pipenv` if necessary: 

```sh
# this could be called "pip", depending on the environment, and must be linked to python 3
pip3 install --user pipenv
```

then install all python dependencies for this project: 

```sh
pipenv install --three --python /Library/Frameworks/Python.framework/Versions/3.6/bin/python3.6
```

The steps below mention to prefix the commands with `pipenv run` whenever necessary in order to have access to those python dependencies. Alternatively, you can enter the corresponding virtualenv once with `pipenv shell`, in which case that prefix is no longer necessary. See [https://docs.pipenv.org](https://docs.pipenv.org) for more details about how to use pipenv to handle python dependencies. 


## Where and how to create a scenario

To create a scenario, simply create another python project that depends on trumania: 

```sh
mkdir -p /path/to/your/project
cd /path/to/your/project

# make sure /path/to/trumania/ is the absolute path where trumania is stored
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
