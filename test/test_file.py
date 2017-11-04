from datetime import datetime
from elkhound import CSVDataFileSpec, CSVInputDataFile, CSVOutputDataFile, Flag
from os.path import dirname, join
from shutil import rmtree
from tempfile import mkdtemp
from unittest import TestCase


class TestDataFile(TestCase):
    def setUp(self):
        self.workspace = mkdtemp(prefix='tmpelkhound')

        test_data = join(dirname(__file__), 'data')
        schema = [('foo', int), ('bar', float), ('baz', str), ('qux', datetime), ('quux', bool)]
        bad_schema = [('foo', int), ('bar', float)]

        self.file_test_read = CSVInputDataFile(
            join(test_data, 'd1000_foo_v20170807000000.csv'),
            'rt',
            CSVDataFileSpec(1000, 'foo', schema=schema))

        self.file_test_read_bad_schema = CSVInputDataFile(
            join(test_data, 'd1000_foo_v20170807000000.csv'),
            'rt',
            CSVDataFileSpec(1000, 'foo', schema=bad_schema))

        self.file_test_read_gz = CSVInputDataFile(
            join(test_data, 'd2000_foo_v20170807000000.csv.gz'),
            'rt',
            CSVDataFileSpec(2000, 'foo', flags=Flag.GZIPPED, schema=schema))

        self.file_workspace_read = CSVInputDataFile(
            join(self.workspace, 'd1000_foo_v20170807000000.csv'),
            'rt',
            CSVDataFileSpec(1000, 'foo', schema=schema))

        self.file_workspace_write = CSVOutputDataFile(
            join(self.workspace, 'd1000_foo_v20170807000000.csv'),
            'wt',
            CSVDataFileSpec(1000, 'foo', schema=schema))

    def tearDown(self):
        rmtree(self.workspace)

    def test_read_data_frame(self):
        df = self.file_test_read.read_data_frame()
        self.assertEqual(len(df), 5)
        self.assertListEqual(list(df.columns), ['foo', 'bar', 'baz', 'qux', 'quux'])
        self.assertListEqual(list(df.baz)[1:4], ['qwe, zxc', 'foo "bar" baz', 'żółć'])

    def test_read_gzipped_data_frame(self):
        df = self.file_test_read_gz.read_data_frame()
        self.assertEqual(len(df), 5)
        self.assertListEqual(list(df.columns), ['foo', 'bar', 'baz', 'qux', 'quux'])
        self.assertListEqual(list(df.baz)[1:4], ['qwe, zxc', 'foo "bar" baz', 'żółć'])

    def test_write_data_frame_without_validation(self):
        df = self.file_test_read.read_data_frame()
        df = df.drop('qux', 1)
        self.file_workspace_write.write_data_frame(df, validate=False)

    def test_write_data_frame_with_validation(self):
        df = self.file_test_read.read_data_frame()
        df = df.drop('qux', 1)
        with self.assertRaises(ValueError):
            self.file_workspace_write.write_data_frame(df, validate=True)

    def test_read_record_stream(self):
        records = list(self.file_test_read.iterate_records())
        self.assertEqual(len(records), 5)
        self.assertEqual(records[3]['baz'], "żółć")

    def test_read_record_stream_without_validation(self):
        list(self.file_test_read_bad_schema.iterate_records(validate=False))

    def test_read_record_stream_with_validation(self):
        with self.assertRaises(AssertionError):
            list(self.file_test_read_bad_schema.iterate_records(validate=True))

    def test_write_read_record_stream(self):
        records = list(self.file_test_read.iterate_records())
        with self.file_workspace_write.get_record_writer() as w:
            for record in records:
                w.write(record)
        records_final = list(self.file_workspace_read.iterate_records())
        self.assertEqual(len(records_final), 5)
        self.assertListEqual(records, records_final)

    def test_write_read_data_frame(self):
        df = self.file_test_read.read_data_frame()
        self.file_workspace_write.write_data_frame(df)
        df_final = self.file_workspace_read.read_data_frame()
        self.assertEqual(len(df_final), 5)
        # Compare carefully, since nan != nan
        df.fillna(0, inplace=True)
        df_final.fillna(0, inplace=True)
        for c in df.columns:
            self.assertListEqual(list(df[c]), list(df_final[c]))
