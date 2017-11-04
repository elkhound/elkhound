import csv
import datetime
import enum
import gzip
import io
import operator
from typing import Any, Dict


class Flag(enum.IntEnum):
    """Flags that can be set for a data file specification."""
    BINARY = 1
    GZIPPED = 2
    DIRECTORY = 4


class DataFileSpec:
    """
    Specification of a data file format.
    """
    def __init__(self, code, name, extension, flags=0):
        """
        Initializes a specification.

        :param code:
        :param name:
        :param extension:
        :param flags:
        """
        self.code = code
        self.name = name
        self.extension = extension
        self.flags = flags

    def is_binary(self) -> bool:
        """
        :return: Whether the file is binary. If the file is gzipped,
                 specifies whether the underlying file (i.e. after unpacking) is binary.
        """
        return bool(self.flags & Flag.BINARY)

    def is_gzipped(self) -> bool:
        """
        :return: Whether the file is zipped using `gzip`.
        """
        return bool(self.flags & Flag.GZIPPED)

    def is_directory(self) -> bool:
        """
        :return: Whether the "file" is actually a directory. If yes,
                 binary and gzipped flags should be ignored.
        """
        return bool(self.flags & Flag.DIRECTORY)

    def is_csv(self) -> bool:
        """
        :return: Whether the file is a CSV file.
        """
        return False


class CSVDataFileSpec(DataFileSpec):
    """
    Specification of a CSV data file format.
    """
    def __init__(self, code, name, extension='csv', flags=0, schema=None, dialect=csv.excel):
        super().__init__(code, name, extension, flags)
        self.schema = schema
        self.dialect = dialect

    def is_csv(self) -> bool:
        """
        :return: Whether the file is a CSV file.
        """
        return True


class DataFile:
    def __init__(self, path: str, mode: str, spec: DataFileSpec):
        self.path = path
        self.mode = mode
        self.spec = spec

    def open(self):
        if self.spec.is_directory():
            raise TypeError("Cannot open a directory")
        if self.spec.is_gzipped():
            f = gzip.open(self.path, mode=self.mode[0] + 'b')
            if self.mode[1] == 't':
                f = io.TextIOWrapper(f, encoding='utf-8', newline='')
            return f
        if self.mode[1] == 't':
            return open(self.path, mode=self.mode, encoding='utf-8', newline='')
        else:
            return open(self.path, mode=self.mode)

    def get_path(self) -> str:
        return self.path


class CSVInputDataFile(DataFile):
    @staticmethod
    def _convert(v, t):
        if t == int:
            try:
                return int(v)
            except ValueError:
                return 0

        # Convert to float
        if t == float:
            try:
                return float(v)
            except ValueError:
                return 0.0

        # Convert to boolean
        if t == bool:
            return (v == 'y') or (v == 'Y') or (v == '1')

        # Convert to string
        if t == str:
            return str(v)

        # Convert to datetime
        if t == datetime.datetime:
            if len(v) == 10:
                return datetime.datetime.strptime(v, '%Y-%m-%d')
            if len(v) == 16:
                return datetime.datetime.strptime(v, '%Y-%m-%d %H:%M')
            if len(v) == 19:
                return datetime.datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
            if len(v) == 20:
                return datetime.datetime.strptime(v, '%Y-%m-%dT%H:%M:%SZ')
            if len(v) == 27:
                return datetime.datetime.strptime(v[0:19], '%Y-%m-%d %H:%M:%S')
            raise ValueError('Unsupported date/time format: %s' % v)

        # Don't know how to convert
        raise TypeError('Unsupported type {}'.format(str(t)))

    def iterate_records(self, validate: bool = True):
        assert self.mode[0] == 'r'
        with self.open() as f:
            reader = csv.reader(f, dialect=self.spec.dialect)
            try:
                header = next(reader)
            except StopIteration:
                if validate:
                    raise RuntimeError("Empty file without header")
                else:
                    return
            if validate:
                assert header == list(map(operator.itemgetter(0), self.spec.schema))
            for record in reader:
                yield {k: self._convert(v, t) for v, (k, t) in zip(record, self.spec.schema)}

    def read_data_frame(self, validate: bool = True):
        assert self.mode[0] == 'r'
        import pandas as pd
        df = pd.read_csv(self.path, sep=self.spec.dialect.delimiter, quotechar=self.spec.dialect.quotechar)
        if validate:
            assert list(df.columns) == list(map(operator.itemgetter(0), self.spec.schema))
        return df


class RecordStreamWriter:
    def __init__(self, data_file: DataFile, validate: bool):
        self.data_file = data_file
        self.validate = validate
        self.header = list(map(operator.itemgetter(0), self.data_file.spec.schema))
        self.bool_columns = {c for c, t in self.data_file.spec.schema if t == bool}

    def __enter__(self):
        self.handle = self.data_file.open()
        self.writer = csv.DictWriter(self.handle, fieldnames=self.header, dialect=self.data_file.spec.dialect)
        self.writer.writeheader()
        return self

    def write(self, record: Dict[str, Any]):
        for c in self.bool_columns:
            record[c] = int(record[c])
        self.writer.writerow(record)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.handle.close()


class CSVOutputDataFile(DataFile):
    def get_record_writer(self, validate: bool = True):
        assert self.mode[0] == 'w'
        return RecordStreamWriter(self, validate)

    def write_data_frame(self, df, validate: bool = True):
        if validate:
            df_columns = list(df.columns)
            schema_columns = list(map(operator.itemgetter(0), self.spec.schema))
            if df_columns != schema_columns:
                raise ValueError('DF columns {} do not match file schema columns {}'.format(
                    str(df_columns),
                    str(schema_columns)
                ))
        df.to_csv(
            self.path,
            sep=self.spec.dialect.delimiter,
            quotechar=self.spec.dialect.quotechar,
            index=False,
            encoding='utf-8',
            compression='gzip' if self.spec.is_gzipped() else None,
        )
