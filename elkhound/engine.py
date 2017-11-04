import abc
import collections
import csv
import datetime
import importlib
import logging
import os
import re
from typing import Dict, List, Optional
import yaml

from .file import CSVDataFileSpec, CSVInputDataFile, CSVOutputDataFile, DataFile, DataFileSpec, Flag


class Task(abc.ABC):
    def __str__(self):
        return '{} -> {}'.format(repr(self.get_input_data_file_codes()), repr(self.get_output_data_file_codes()))

    @abc.abstractmethod
    def get_input_data_file_codes(self) -> List[int]:
        pass

    @abc.abstractmethod
    def get_output_data_file_codes(self) -> List[int]:
        pass

    @abc.abstractmethod
    def run(self, input_files: Dict[int, DataFile], output_files: Dict[int, DataFile], context=None):
        pass


class Engine:
    def __init__(self, timestamp: Optional[int] = None):
        self.specs = dict()
        self.tasks_by_target = dict()
        self.workflows = dict()
        self.timestamp = timestamp if timestamp is not None else int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

    def register_file_spec(self, spec: DataFileSpec):
        log = logging.getLogger(__name__)
        if spec.code in self.specs:
            raise ValueError("File specification {} already registered".format(spec.code))
        log.debug("Registering file spec {} ({})".format(spec.code, spec.name))
        self.specs[spec.code] = spec

    def register_task(self, task: Task):
        log = logging.getLogger(__name__)
        for code in task.get_output_data_file_codes():
            if code in self.tasks_by_target:
                raise ValueError("Task with output {} already registered".format(code))

        # Check that max(input codes) < min(output codes)
        if task.get_input_data_file_codes() and task.get_output_data_file_codes():
            max_input_code = max(task.get_input_data_file_codes())
            min_output_code = min(task.get_output_data_file_codes())
            if max_input_code >= min_output_code:
                raise ValueError("Input code {} not smaller than output code {} in task {}".format(
                    max_input_code, min_output_code, task))

        for code in task.get_input_data_file_codes():
            if code not in self.specs:
                raise ValueError("Unregistered spec {} referenced in task {}".format(code, task))

        for code in task.get_output_data_file_codes():
            if code not in self.specs:
                raise ValueError("Unregistered spec {} referenced in task {}".format(code, task))

        log.debug("Registering task {}".format(task))
        for code in task.get_output_data_file_codes():
            self.tasks_by_target[code] = task

    def register_workflow(self, name: str, tasks: List[int]):
        log = logging.getLogger(__name__)
        if name in self.workflows:
            raise ValueError("Workflow {} already registered".format(name))
        log.debug("Registering workflow {} with tasks {}".format(name, str(tasks)))
        self.workflows[name] = tasks

    @staticmethod
    def _get_class_by_name(fully_qualified_class_name):
        items = fully_qualified_class_name.split('.')
        module_name = '.'.join(items[0:-1])
        class_name = items[-1]
        return getattr(importlib.import_module(module_name), class_name)

    def read(self, config_file_name):
        with open(config_file_name) as f:
            engine_config = yaml.load(f)

            # Register file specs
            for spec in engine_config['specs']:
                code = int(spec['code'])
                name = spec['name']
                extension = spec.get('extension', 'csv')
                # Create flags
                flags = 0
                for flag in spec.get('flags', []):
                    flags |= getattr(Flag, flag.upper())
                if 'schema' in spec:
                    # Create schema
                    schema = []
                    types = {'bool': bool, 'datetime': datetime.datetime, 'float': float, 'int': int, 'str': str}
                    for item in spec['schema']:
                        schema.append((item['name'], types[item['type']]))
                    # Create dialect
                    dialect = type('dialect{}'.format(code), (csv.excel,), dict())
                    for a in spec.get('dialect', dict()):
                        setattr(dialect, a, spec['dialect'][a])
                    # Register file spec
                    self.register_file_spec(CSVDataFileSpec(code, name, extension, flags, schema, dialect))
                else:
                    self.register_file_spec(DataFileSpec(code, name, extension, flags))

            # Register tasks
            for task in engine_config['tasks']:
                cls = self._get_class_by_name(task['class'])
                self.register_task(cls())

            # Register workflows
            for workflow_name in engine_config.get('workflows', dict()):
                workflow_tasks = list(map(int, engine_config['workflows'][workflow_name]))
                self.register_workflow(workflow_name, workflow_tasks)

    def expand_targets(self, targets: List[str], dependencies: bool = False) -> List[int]:
        # Expand workflow references
        tasks = []
        for target in targets:
            if target in self.workflows:
                tasks += self.workflows[target]
            else:
                tasks.append(int(target))

        # Add upstream targets if required
        if dependencies:
            tasks = self._add_dependencies(tasks)

        return tasks

    def _get_task(self, target: int) -> Task:
        if target not in self.tasks_by_target:
            raise ValueError("No registered task that can create target {}".format(target))
        return self.tasks_by_target[target]

    def _add_dependencies(self, tasks: List[int]) -> List[int]:
        queue = collections.deque(tasks)
        extended_targets = []
        while queue:
            target = queue.pop()
            extended_targets.append(target)
            task = self._get_task(target)
            dependencies = task.get_input_data_file_codes()
            for dependency in dependencies:
                if dependency in extended_targets or dependency in queue:
                    continue
                queue.appendleft(dependency)
        return list(sorted(extended_targets))

    def _to_data_file(self, workspace: str, rw: str, code: int) -> DataFile:
        assert rw in ['r', 'w']
        spec = self.specs[code]
        tb = 'b' if spec.is_binary() else 't'

        version = self.timestamp
        if rw == 'r':
            versions = set()
            for file_name in os.listdir(workspace):
                pattern = r'd{:04}_{}_v(\d+).{}'.format(code, spec.name, spec.extension)
                m = re.match(pattern, file_name)
                if m:
                    versions |= {int(m.group(1))}
            if not versions:
                raise RuntimeError("No input files for code {:04}".format(code))
            # If we see files with timestamps of the current run, use them.
            # This will help us when there are several runs in parallel in the same workspace.
            elif self.timestamp in versions:
                version = self.timestamp
            else:
                version = max(versions)
        path = os.path.join(workspace, 'd{:04}_{}_v{}.{}'.format(code, spec.name, version, spec.extension))
        if spec.is_csv():
            if rw == 'r':
                return CSVInputDataFile(path, rw + tb, spec)
            else:
                return CSVOutputDataFile(path, rw + tb, spec)
        else:
            return DataFile(path, rw + tb, spec)

    def run(self, workspace, targets, context=None):
        log = logging.getLogger(__name__)

        if not context:
            context = dict()
        log.debug("Context has {} items".format(len(context)))
        for k, v in context.items():
            log.debug("* {}: {}".format(k, str(v)))

        completed_tasks = set()
        for target in targets:
            task = self._get_task(target)
            if task in completed_tasks:
                continue
            log.info("Building target {} ({}) by running task {}".format(target, self.specs[target].name, task))
            input_files = dict()
            for code in task.get_input_data_file_codes():
                input_files[code] = self._to_data_file(workspace, 'r', code)
                log.debug("Input file {} is {}".format(code, input_files[code].get_path()))
            output_files = dict()
            for code in task.get_output_data_file_codes():
                output_files[code] = self._to_data_file(workspace, 'w', code)
                log.debug("Output file {} is {}".format(code, output_files[code].get_path()))
            task.run(input_files, output_files, context)
            completed_tasks |= {task}
