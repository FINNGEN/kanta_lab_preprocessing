from kanta import config
from kanta.engine import injection

if __name__ == '__main__':
    injection.fake_run(config.EXAMPLE_VAR)
