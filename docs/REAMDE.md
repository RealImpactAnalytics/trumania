# How to generate the wiki and the doc

You need to have have two copy of the trumania repository, one where you work on the code and one where you generate the docs.
The one with the docs should be trumania-docs/html.
The structure should be
```
- trumania (repository on the master branch)
- trumania-docs
    - html (repository on the branch gh_pages
```

Once you have the correct structure, go to `trumania/docs` and run the two following commands
```
# Only required if the code api changed, it will update the code structure
sphinx-apidoc ../trumania -o source

# It will update the html pages
make html
```
