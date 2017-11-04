from elkhound.runner import run_engine
from sys import argv


# Alternatively, run the following from command line: python -m elkhound.runner --engine engine.yaml --conf ...

argv[1:] = [
    '--engine', 'engine.yaml',
    '--conf', 'conf.ini',
    '--dir', '.',
    '--targets', 'monthly_briefing', '--deps',
]

run_engine()
