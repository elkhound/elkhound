from elkhound import Task, run_engine
from os import listdir
from os.path import join
from shutil import rmtree
from sys import argv
from tempfile import mkdtemp
from unittest import TestCase


class TaskFoo(Task):
    def get_input_data_file_codes(self):
        return []

    def get_output_data_file_codes(self):
        return [1000]

    def run(self, input_files, output_files, context=None):
        context['roll_call'] = ['foo']
        for output_file in output_files.values():
            with output_file.get_record_writer():
                pass


class TaskBar(Task):
    def get_input_data_file_codes(self):
        return [1000]

    def get_output_data_file_codes(self):
        return [2000]

    def run(self, input_files, output_files, context=None):
        context['roll_call'] += ['bar']


class TaskBaz(Task):
    def get_input_data_file_codes(self):
        return [2000]

    def get_output_data_file_codes(self):
        return [3000]

    def run(self, input_files, output_files, context=None):
        context['roll_call'] += ['baz']


class TaskBazAlternative(Task):
    def get_input_data_file_codes(self):
        return [1000]

    def get_output_data_file_codes(self):
        return [3000]

    def run(self, input_files, output_files, context=None):
        fake = context['baz_alternative.fake']
        context['roll_call'] += ['baz_alternative', fake]
        for output_file in output_files.values():
            with output_file.open():
                pass


class TaskReportAndPlots(Task):
    def get_input_data_file_codes(self):
        return [3000]

    def get_output_data_file_codes(self):
        return [9100, 9200]

    def run(self, input_files, output_files, context=None):
        context['roll_call'] += ['report_and_plots']


class TaskSummary(Task):
    def get_input_data_file_codes(self):
        return []

    def get_output_data_file_codes(self):
        return [9900]

    def run(self, input_files, output_files, context=None):
        context['roll_call'] += ['summary']
        with output_files[9900].open() as f:
            f.write(' '.join(context['roll_call']))


class TestRunEngine(TestCase):
    def setUp(self):
        self.workspace = mkdtemp(prefix='tmpelkhound')

    def tearDown(self):
        rmtree(self.workspace)

    def test_run_engine(self):
        argv_copy = argv.copy()
        argv[1:] = [
            '--engine', 'mock_engine.yaml',
            '--dir', self.workspace,
            '--targets', 'deliverables', '9900', '--deps',
            '--conf', 'mock_params.ini',
        ]
        try:
            run_engine(logs=False)
        finally:
            argv[1:] = argv_copy[1:]

        name = None
        for item in listdir(self.workspace):
            if '9900' in item:
                name = item
                break
        self.assertIsNotNone(name)
        with open(join(self.workspace, name)) as f:
            roll_call = f.readline().strip().split(' ')
        self.assertListEqual(roll_call, ['foo', 'baz_alternative', 'quux', 'report_and_plots', 'summary'])
