from .file import CSVDataFileSpec, DataFileSpec, Flag
from .file import CSVInputDataFile, CSVOutputDataFile, DataFile
from .engine import Task, Engine
from .runner import run_engine


__doc__ = "Elkhound is an opinionated, data-centric workflow engine."

__all__ = [
    'CSVDataFileSpec', 'DataFileSpec', 'Flag',
    'CSVInputDataFile', 'CSVOutputDataFile', 'DataFile',
    'Task', 'Engine',
    'run_engine',
]
