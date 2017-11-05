.. image:: https://travis-ci.org/elkhound/elkhound.svg?branch=master
    :target: https://travis-ci.org/elkhound/elkhound

.. image:: https://img.shields.io/codecov/c/github/elkhound/elkhound/master.svg?style=flat
    :target: https://codecov.io/gh/elkhound/elkhound?branch=master

.. image:: https://readthedocs.org/projects/elkhound/badge/?version=latest
    :target: http://elkhound.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

Elkhound is an opinionated, data-centric workflow engine.  It makes the following assumptions about your project:

* Project workflow can be split into several tasks, each task has clearly defined input and output data files, ideally with no side effects. Tasks form a directed acyclic graph (i.e., no loops).
* Each data file has clearly defined format and schema. Information between consecutive tasks is transferred primarily via the files. Preference for CSV or gzipped CSV files.

Elkhound will help you by:

* **Versioning** data files by timestamping them (tasks read input files with the latest timestamp, write output files with current timestamp).
* Supporting **big data files** by providing convenience schema-aware iterators over rows (using Python generators), lowering memory footprint.
* Managing **workflows** of tasks, easily running arbitrary lists of tasks.
* Automatic **checkpointing** of intermediate results, thanks to inter-task communication via data files and data file versioning.
* Managing project's parameters by injecting contents of **configuration files and command line parameters** as tasks' contexts (less temptation to hardcode constants).
* **Logging** workflow executions, reporting context and execution progress. Automatically collecting and archiving logs from different runs in one place to facilitate reproducibility.
* Facilitating **unit testing** by injecting mockable data file objects as inputs and outputs of task classes.

Getting started
---------------

Install Elkhound by running:

.. code:: bash

   pip install elkhound

In order to run Elkhound workflows, you need to create
an engine configuration file
and implement business logic in ``Task`` subclasses.

Engine configuration
--------------------

Engine configuration file  (in our example we'll name it ``engine.yaml``)
has three sections:

* ``specs``, where you'll describe data files (names, formats, schemas; these files are outputs for some tasks, inputs for other tasks),
* ``tasks``, where you'll point to implementations of business logic. Tasks define transformations of data files (how to build an output file Z given input files X and Y)
* ``workflows``, where you'll bundle groups of target data files.

Data files
~~~~~~~~~~

Data files are first class citizens in Elkhound.
They are identified by four digit codes (e.g. ``1230``, ``2110``, ``4315``, ``5214``).
Design of a new workflow should begin with registering
data files, and optionally defining their schemas (if applicable).
Here is an example of data files defined in an engine configuration file:

.. code:: yaml

    specs:
      - code: 1230
        name: people
        extension: csv.gz
        flags:
          - gzipped
        schema:
          - name: name
            type: str
          - name: dob
            type: datetime
          - name: is_employee
            type: bool
      - code: 2110
        name: budget
        extension: xlsx
        flags:
          - binary
      - code: 4315
        name: report
        extension: txt
      - code: 5214
        name: plots
        extension: dir
        flags:
          - directory

Tasks
~~~~~

Tasks are Python classes that take zero or more data files on input
and produce zero or more data files on output.
Each task class has to implement three methods:

* ``get_input_data_file_codes(self)`` returning a list of input data file codes,
* ``get_output_data_file_codes(self)`` returning a list of output data file codes,
* ``run(self, input_files, output_files, context)`` executing business logic.

Here is an example of tasks registered in an engine configuration file:

.. code:: yaml

    tasks:
      - class: myapp.DownloadDataTask
      - class: myapp.GenerateReportTask
      - class: myapp.PlotBudgetTask

In our example we will assume that:

* ``DownloadDataTask`` takes no data files on input, produces ``1230`` and ``2110`` on output.
* ``GenerateReportTask`` takes ``1230`` and ``2110`` on input, produces ``4315`` on output.
* ``PlotBudgetTask`` takes ``2110`` on input, produces ``5214`` on output.

Workflows
~~~~~~~~~

Workflows are named lists of targets, i.e., data files to be created.
Here is an example (excerpt of an engine configuration file):

.. code:: yaml

    workflows:
      monthly_briefing:
        - 4315
        - 5214

Business logic implementation
-----------------------------

Each task is implemented as a subclass of ``elkhound.Task``.
Their task is to read the input files they need and create
the output files.
Here is a simple example:

.. code:: python

    class GenerateReportTask(Task):
        def get_input_data_file_codes(self):
            return [1230, 2110]

        def get_output_data_file_codes(self):
            return [4315]

        def run(self, input_files, output_files, context=None):
            with output_files[4315].open() as f:
                for _, input_file in input_files.items():
                    f.write('Used input file {}\n'.format(input_file.get_path()))

When method ``run`` is called by the engine,
the ``input_files`` and ``output_files`` arguments
contain ``DataFile`` objects that know the exact path of the files
and can assist in opening them in the right mode (read or write, text or binary, gzipped or not).
Data file objects have utility methods for specific situations,
for example when an input file is in CSV format, the corresponding data file object
has methods like ``read_data_frame()`` that returns a Pandas data frame,
and ``iterate_records()`` which returns a generator yielding records one-by-one
(useful when scanning huge files that won't fit into memory).

Running workflows
-----------------

Here's an example of how to run a workflow:

.. code:: bash

   python -m elkhound.runner --dir /workspace/foo --engine engine.yaml --targets monthly_briefing --deps
