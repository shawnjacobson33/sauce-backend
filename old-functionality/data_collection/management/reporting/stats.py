import json

from app import LineWorkerStats
from app import utils as rp_utils


def report_line_worker_stats():
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("line_workers", is_secondary=True, secondary_type="stats")
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        line_worker_stats = rp_utils.convert_deque_and_keys(LineWorkerStats.get_stats())
        # save the betting lines to the file, in pretty print mode
        json.dump(line_worker_stats, f, indent=4, default=str)