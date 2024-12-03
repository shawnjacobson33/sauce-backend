import threading
from collections import defaultdict, deque


class LineWorkerStats:
    _stats: dict = defaultdict(dict)
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def get_stats(cls, source: str = None):
        return cls._stats.get(source) if source else cls._stats

    @classmethod
    def update_line_counts(cls, source: str, batch_id: str = None, **kwargs):
        """
        new_lines, line_changes, completed_lines, total_lines, time, total_time, avg_time
        """
        with cls._lock:
            source_stats = cls._stats[source]
            if 'batches' not in source_stats:
                cls._start_new_batch(source, batch_id)

            most_recent_batch = source_stats['batches'][-1]

            for key, val in kwargs.items():
                if 'total' in key:
                    source_stats.setdefault(key, 0)
                    source_stats[key] += val
                    continue

                most_recent_batch.setdefault(key, 0)
                most_recent_batch[key] += val
                source_stats.setdefault(f'total_{key}', 0)
                source_stats[f'total_{key}'] += val

    @classmethod
    def _start_new_batch(cls, source: str, batch_id: str):
        """Should include a batch id and group"""
        cls._stats[source]['batches'] = deque([{ 'batch_id': batch_id }])

    @classmethod
    def update_request_counts(cls, source: str, **kwargs):
        """successful_requests, failed_requests, total_requests"""
        with cls._lock:
            for key, val in kwargs.items():
                if 'total' in key:
                    cls._stats[source].setdefault(key, 0)
                    cls._stats[source][key] += val
                    continue

                cls._stats[source].setdefault(f'total_{key}', 0)
                cls._stats[source][f'total_{key}'] += val