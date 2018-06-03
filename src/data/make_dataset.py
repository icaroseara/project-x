# -*- coding: utf-8 -*-
import click
import csv
import logging
import os.path

from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from pynubank import Nubank
from shutil import copyfile

EXTERNAL_DATA_DIR = 'data/external'
RAW_DATA_DIR = 'data/raw'
PROCESSED_DATA_DIR = 'data/processed'


def _download_external_data():
    externa_data_file = '{}/card_statements.csv'.format(EXTERNAL_DATA_DIR)
    if not os.path.exists(externa_data_file):
        nu = Nubank(os.environ.get('NUBANK_LOGIN'), os.environ.get('NUBANK_PASSWORD'))
        _card_statements_to_csv(nu.get_card_statements(), externa_data_file)


def _card_statements_to_csv(card_statements, externa_data_file):
    with open(externa_data_file, 'w') as f:
        field_names = card_statements[0].keys()
        dict_writer = csv.DictWriter(f, field_names)
        dict_writer.writeheader()
        dict_writer.writerows(card_statements)


def _create_raw_data_file(external_data_file):
    raw_data_file = '{}/card_statements.csv'.format(RAW_DATA_DIR)
    if not os.path.exists(raw_data_file):
        copyfile(external_data_file, raw_data_file)


def _create_processed_data_file(raw_data_file):
    processed_data_file = '{}/card_statements.csv'.format(PROCESSED_DATA_DIR)
    if not os.path.exists(processed_data_file):
        copyfile(raw_data_file, processed_data_file)


@click.command()
def main():
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('downloading data from external source')
    _download_external_data()

    logger.info('making raw data from external data')
    _create_raw_data_file('{}/card_statements.csv'.format(EXTERNAL_DATA_DIR))

    logger.info('making final data set from raw data')
    _create_processed_data_file('{}/card_statements.csv'.format(RAW_DATA_DIR))


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())
    main()
