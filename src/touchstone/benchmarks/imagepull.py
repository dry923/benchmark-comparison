import logging


from . import BenchmarkBaseClass


logger = logging.getLogger("touchstone")


class Imagepull(BenchmarkBaseClass):
    def _build_search(self):
        logger.debug("Building search array for Image Pull data")
        return self._search_dict[self._source_type][self._harness_type]

    def _build_search_metadata(self):
        return self._search_dict[self._source_type]["metadata"]

    def _build_compute(self):
        logger.debug("Building Image Pull compute map")
        _temp_dict = {}
        for index in self._search_map:
            _temp_dict[index] = self._search_map[index]
        return _temp_dict

    def __init__(self, source_type=None, harness_type=None, config=None):
        logger.debug("Initializing Image Pull data instance")
        BenchmarkBaseClass.__init__(
            self, source_type=source_type, harness_type=harness_type, config=config
        )
        self._search_dict = {
            "elasticsearch": {
                "metadata": {},
                "ripsaw": {
                    "image-pull-results": [
                        {
                            "filter": {},
                            "buckets": [
                                "image.keyword",
                                "pod_count.keyword"
                            ],
                            "aggregations": {"failures": ["sum"], "successful": ["sum"]},
                        },
                        {
                            "filter": {"failures": "0"},
                            "buckets": [
                                "image.keyword",
                                "pod_count.keyword"
                            ],
                            "aggregations": {"elapsed_time": ["min", "max", "avg", {"percentiles": {"percents": [95]}}]},
                        },
                        {
                            "filter": {"failures": "1"},
                            "buckets": [
                                "pod_name.keyword"
                            ],
                            "aggregations": {"elapsed_time": ["min"]}
                        }
                    ],
                },
            },
        }
        if self.benchmark_cfg:
            self._search_dict = self.benchmark_cfg
        self._search_map = self._build_search()
        self._search_map_metadata = self._build_search_metadata()
        self._compute_map = self._build_compute()
        logger.debug("Finished initializing Impage Pull data instance")

    def emit_compute_map(self):
        logger.debug("Emitting built compute map ")
        logger.info(
            "Compute map is {} in the database {}".format(
                self._compute_map, self._source_type
            )
        )
        return self._compute_map

    def emit_indices(self):
        return self._search_map.keys()

    def emit_metadata_search_map(self):
        return self._search_map_metadata
