from setuptools import setup, find_packages

with open('./requirements.txt') as f:
    install_requires = f.read().strip().split('\n')

setup(
    name='trumania',
    version='1.0',
    install_requires=install_requires,
    packages=find_packages(),
    py_modules=find_packages()
)
