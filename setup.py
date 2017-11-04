from setuptools import setup


with open('README.rst') as f:
    long_description = f.read()

setup(
    name = "elkhound",
    packages = ["elkhound"],
    version = "0.0.4",
    description = "Elkhound workflow engine",
    author = "Lukasz Bolikowski",
    author_email = "lukasz@bolikowski.eu",
    url = "https://github.com/elkhound/elkhound",
    keywords = ["data", "workflow", "engine"],
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Development Status :: 3 - Alpha",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    long_description = long_description,
    install_requires = [
        "PyYAML",
    ],
)
