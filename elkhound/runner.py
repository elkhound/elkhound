import argparse
import configparser
import datetime
import logging

from .engine import Engine
from .logger import DummyLogger, FileLogger


def _parse_args():
    parser = argparse.ArgumentParser(description='Run a workflow.')
    parser.add_argument('--dir', type=str, required=True,
                        help='a path to the data directory')
    parser.add_argument('--targets', nargs='+', required=True,
                        help='targets to run')
    parser.add_argument('--deps', action='store_true',
                        help='add upstream targets (resolve dependencies)')
    parser.add_argument('--engine', required=False, default='engine.yaml',
                        help='engine configuration file in YAML format')
    parser.add_argument('--conf', nargs='+', required=False, default=None,
                        help='parameter file(s) in INI format')
    parser.add_argument('--params', nargs='+', required=False, default=[],
                        help='additional parameters from command line')
    return parser.parse_args()


def read_context(param_file_paths, command_line_params):
    context = dict()
    if not param_file_paths:
        param_file_paths = []
    for param_file_path in param_file_paths:
        if param_file_path is None:
            continue
        config = configparser.ConfigParser()
        config.read(param_file_path)
        for section in config:
            for key in config[section]:
                context[section + '.' + key] = config[section][key]
    for param in command_line_params:
        if '=' in param:
            key, value = param.split('=')
            if '.' in key:
                section, key = key.split('.', 2)
            else:
                section = 'default'
            context[section + '.' + key.lower()] = value
    return context


def run_engine(timestamp: int = None, callback=None, logs: bool = True):
    """
    Set up and run an engine instance. Read config files, parse command-line arguments,
    register file specs and tasks found in the config, set up logging, etc. and run the engine.

    It is suggested to run this function in the main function.
    If additional tweaking is needed between setting up the engine and running it,
    provide a callback function.

    :param timestamp: Optional timestamp of the run, as integer in YYYYMMDDHHMMSS format.
    :param callback: Optional function to call just before running the engine.
           The callback function will receive two arguments: the configured engine instance,
           and a dictionary of arguments that were about to be passed to the engine's `run()` method.
    :param logs: Whether to configure logging.
    """

    # Set timestamp
    if timestamp is None:
        timestamp = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

    # Read command-line parameters
    args = _parse_args()

    # Set up logging
    logger = FileLogger(args.dir, timestamp) if logs else DummyLogger()
    log = logging.getLogger(__name__)

    # Set up engine
    log.info("Setting up engine")
    engine = Engine(timestamp=timestamp)
    engine.read(args.engine)
    targets = engine.expand_targets(args.targets, args.deps)

    # Set up context
    log.info("Setting up context")
    context = read_context(args.conf, args.params)

    # Run the engine
    arguments = dict(
        workspace=args.dir,
        context=context,
        targets=targets,
    )
    if callback:
        log.info("Executing callback")
        arguments = callback(engine, arguments)
    logger.report_start(timestamp, targets, context)
    log.info("Running the engine")
    try:
        engine.run(**arguments)
        logger.report_finish(timestamp, True)
    except Exception as e:
        logger.report_finish(timestamp, False)
        log.exception('Exception caught')
        raise e


if __name__ == '__main__':
    run_engine()
