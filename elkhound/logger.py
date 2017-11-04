import abc
import datetime
import logging.config
import os
from typing import Dict, List


class Logger(abc.ABC):
    @abc.abstractmethod
    def report_start(self, timestamp: int, targets: List[int], params: Dict[str, str]):
        pass

    @abc.abstractmethod
    def report_finish(self, timestamp: int, success: bool):
        pass


class DummyLogger(Logger):
    def report_start(self, timestamp: int, targets: List[int], params: Dict[str, str]):
        pass

    def report_finish(self, timestamp: int, success: bool):
        pass


class FileLogger(Logger):
    def __init__(self, workspace, timestamp):
        self.workspace = workspace
        self.timestamp = timestamp
        os.makedirs(os.path.join(self.workspace, 'log'), exist_ok=True)
        self.runs_log = os.path.join(self.workspace, 'log', 'runs.log')
        self._set_up_logging()

    def _set_up_logging(self):
        logging_config = {
            'version': 1,
            'formatters': {
                'basic': {
                    'format': '%(asctime)-15s %(name)s %(levelname)s %(message)s'
                }
            },
            'handlers': {
                'stream': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'basic'
                },
                'file': {
                    'class': 'logging.FileHandler',
                    'formatter': 'basic',
                    'filename': os.path.join(self.workspace, 'log', str(self.timestamp) + '.log')
                }
            },
            'root': {
                'level': 'DEBUG',
                'handlers': ['stream', 'file']
            }
        }
        logging.config.dictConfig(logging_config)

    def report_start(self, timestamp: int, targets: List[int], params: Dict[str, str]):
        if not os.path.isfile(self.runs_log):
            with open(self.runs_log, 'wt') as f:
                f.write('run_id|timestamp|status|targets|params\n')

        with open(self.runs_log, 'at') as f:
            f.write('%d|%s|%s|%s|%s\n' % (
                timestamp,
                datetime.datetime.strptime(str(timestamp), '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S'),
                'START',
                ' '.join(map(str, targets)),
                ' '.join(['%s=%s' % (k, params[k].replace(' ', '_')) for k in params if type(params[k]) == str])
            ))

    def report_finish(self, timestamp: int, success: bool):
        with open(self.runs_log, 'at') as f:
            f.write('%d|%s|%s|||\n' % (
                timestamp,
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                {True: 'FINISH', False: 'CRASH'}[success],
            ))
