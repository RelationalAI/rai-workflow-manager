import logging
import os

from datetime import datetime, timedelta
from typing import List, Dict

from workflow import constants


def range_days(start: datetime, end: datetime) -> List[datetime]:
    delta = end - start
    return [start + timedelta(days=i) for i in range(delta.days + 1)]


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, constants.DATE_FORMAT)


def get_date_path(end_date: str, date_format: str, day_offset: int):
    date = datetime.strptime(end_date, date_format) - timedelta(days=day_offset)
    return f"{constants.DATE_PREFIX}{date.strftime(date_format)}"


def read(fname: str) -> str:
    with open(fname) as fp:
        return fp.read()


def sansext(fname: str) -> str:
    return os.path.splitext(os.path.basename(fname))[0]


def save_csv_output(outputs: Dict, output_root: str) -> None:
    """
    Save the content of dictionary as CSV files
    :param outputs:     dictionary with outputs
    :param output_root: output root folder
    :return:
    """
    for output in outputs.keys():
        # for the time being, this handles the specialized relations of meta-exports
        normalized_file_name = output.replace("/:", "_")
        with open(f"{output_root}/{normalized_file_name}.csv", "w") as file:
            file.write(outputs[output])


def build_relation_path(relation: str, *keys: str) -> str:
    """
    Build relation from base and paths.
    Ex. base:`batch:config`, paths: ['daily', 'fake'] => `batch:config:daily:fake`
    :param relation:    base relation
    :param keys:        path parts
    :return: new relation
    """
    relation_path = ":" + ":".join(keys) if len(keys) > 0 else ""
    return f"{relation}{relation_path}"


def build_models(filenames: List[str], files_root: str) -> dict:
    """
    Build RAI models from list of files.
    :param filenames:   file names
    :param files_root:  files root path
    :return:
    """
    models = {}
    for file in filenames:
        filename = f"{files_root}/{file}"
        with open(filename) as fp:
            models[file] = fp.read()
    return models


def format_duration(seconds: float) -> str:
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    hours_int = int(hours)
    minutes_int = int(minutes)
    seconds_int = int(seconds)
    if hours_int != 0:
        return f"[{hours_int:d}h {minutes_int:d}m {seconds_int:d}s]"
    elif minutes_int != 0:
        return f"[{minutes_int:d}m {seconds_int:d}s]"
    else:
        return f"[{seconds_int:d}s]"


def get_common_model_relative_path(file) -> str:
    """
    Get relative path to common model from folder of given file.
    :param file:    file
    :return:
    """
    return os.path.dirname(os.path.realpath(file)) + constants.COMMON_MODEL_RELATIVE_PATH


def extract_date_range(logger: logging.Logger, start_date_str, end_date_str, number_of_days, offset_of_days) -> List[
    str]:
    end_date = parse_date(end_date_str)
    offset = offset_of_days if offset_of_days is not None else 0
    end_date = end_date - timedelta(offset)
    start_date = parse_date(start_date_str) if start_date_str is not None else end_date
    start_date_adjusted = end_date - timedelta(number_of_days - 1) if number_of_days is not None else start_date
    start_date = start_date if start_date > start_date_adjusted else start_date_adjusted

    logger.info(f"Building range from '{start_date}' to '{end_date}'")
    return [date.strftime(constants.DATE_FORMAT) for date in range_days(start_date, end_date)]

