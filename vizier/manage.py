import logging
import logging.config
import os

import click
import pkg_resources

from vizier.config import PACKAGE, CONFIG_DIR_NAME, LOGGING_CONF_FILE_NAME


@click.group(name='main')
@click.option('--verbose', is_flag=True, help='Sets logging level to `DEBUG`.')
def main(verbose: bool):
    resource_manager = pkg_resources.ResourceManager()
    conf_dir = resource_manager.resource_filename(PACKAGE, CONFIG_DIR_NAME)
    logging_conf_file_path = os.path.join(conf_dir, LOGGING_CONF_FILE_NAME)
    set_logging(logging_conf_file_path, verbose)


def set_logging(logging_conf_file_path: str, verbose: bool):
    logging.config.fileConfig(logging_conf_file_path)
    if not verbose:
        logging.getLogger().setLevel(logging.INFO)


@main.command(name='run')
def run():
    logging.info('Running "Vizier" service.')


if __name__ == '__main__':
    main()
