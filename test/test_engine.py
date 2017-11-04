from elkhound import DataFileSpec, Engine, Task
from os.path import join
from shutil import rmtree
from tempfile import mkdtemp
from unittest import TestCase


class MockTask(Task):
    def __init__(self, code, input_specs, output_specs):
        self.code = code
        self.input_specs = input_specs
        self.output_specs = output_specs

    def get_input_data_file_codes(self):
        return self.input_specs

    def get_output_data_file_codes(self):
        return self.output_specs

    def run(self, input_files, output_files, context=None):
        for input_file in input_files.values():
            context[input_file.spec.code] = input_file.path
        for output_file in output_files.values():
            with output_file.open() as f:
                pass
        context['roll_call'].append(self.code)


class TestEngine(TestCase):
    def setUp(self):
        self.workspace = mkdtemp(prefix='tmpelkhound')
        self.engine = Engine(timestamp=20170808000000)

        # Create mock file specs
        for code in range(1000, 9000, 100):
            self.engine.register_file_spec(DataFileSpec(code, 'foo', 'dat'))

        # Create mock tasks
        self.engine.register_task(MockTask('A', [], [1000, 1100]))
        self.engine.register_task(MockTask('B1', [1000], [2000]))
        self.engine.register_task(MockTask('C', [1000], [3000]))
        self.engine.register_task(MockTask('D', [1000, 2000], [4000]))
        self.engine.register_task(MockTask('E5', [], [5000]))
        self.engine.register_task(MockTask('F', [3000], [6000]))

    def tearDown(self):
        rmtree(self.workspace)

    def test_dependencies_off(self):
        context = {'roll_call': []}
        for code in [1000, 2000]:
            with open(join(self.workspace, 'd{:04}_foo_v20170807000000.dat'.format(code)), 'w') as f:
                pass
        self.engine.run(self.workspace, [4000], context)
        self.assertListEqual(context['roll_call'], ['D'])

    def test_dependencies_on(self):
        context = {'roll_call': []}
        tasks = self.engine.expand_targets(['4000'], True)
        self.assertListEqual(tasks, [1000, 2000, 4000])
        self.engine.run(self.workspace, tasks, context)
        self.assertListEqual(context['roll_call'], ['A', 'B1', 'D'])

    def test_missing_task(self):
        with self.assertRaises(ValueError):
            self.engine.run(self.workspace, [1111], None)

    def test_repeated_spec(self):
        with self.assertRaises(ValueError):
            self.engine.register_file_spec(DataFileSpec(2000, 'bar', 'xlsx'))

    def test_same_priorities(self):
        with self.assertRaises(ValueError):
            self.engine.register_task(MockTask('Z', [], [4000]))

    def test_topological_ordering(self):
        with self.assertRaises(ValueError):
            self.engine.register_task(MockTask('Z', [1000, 2000, 4000], [3000, 5000]))

    def test_takes_latest_file(self):
        context = {'roll_call': []}
        for version in [20170807000000, 20180101000000, 20140101000000]:
            with open(join(self.workspace, 'd3000_foo_v{}.dat'.format(version)), 'wt'):
                pass
        self.engine.run(self.workspace, [6000], context)
        self.assertEqual(context[3000], join(self.workspace, 'd3000_foo_v{}.dat'.format(20180101000000)))

    def test_takes_timestamp_file(self):
        context = {'roll_call': []}
        for version in [20170808000000, 20180101000000, 20140101000000]:
            with open(join(self.workspace, 'd3000_foo_v{}.dat'.format(version)), 'wt'):
                pass
        self.engine.run(self.workspace, [6000], context)
        self.assertEqual(context[3000], join(self.workspace, 'd3000_foo_v{}.dat'.format(20170808000000)))
