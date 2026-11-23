from prometheus_client import Counter, Gauge, Histogram

LAG = Gauge('consumer_lag', 'Latest offset minus committed offset', ['topic', 'partition', 'group'])
COMMITTED = Gauge('consumer_committed_offset', 'Next offset to read according to group commits', ['topic', 'partition', 'group'])
HEAD = Gauge('consumer_head_offset', 'Next offset after the latest record', ['topic', 'partition', 'group'])
PROCESSED = Counter('events_processed_total', 'Successfully applied warehouse events', ['event_type'])
RESULTS = Counter('event_outcomes_total', 'Consumer decisions', ['outcome'])
DURATION = Histogram('event_processing_duration_seconds', 'Time spent handling one delivery',
                     buckets=(.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30))
WRITE_ERRORS = Counter('cassandra_write_errors_total', 'Failed Cassandra write attempts')
DEPENDENCY = Gauge('dependency_up', 'Availability of a dependency', ['dependency'])
