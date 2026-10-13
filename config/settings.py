import os

# Thresholds
SMALL_FILE_THRESHOLD_MB = 200

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "ecommerce_db"

# Collections
COLLECTION_RAW = "orders_raw"
COLLECTION_VALIDATED = "orders_validated"
COLLECTION_QUARANTINE = "orders_quarantine"
COLLECTION_METRICS = "pipeline_metrics"

# Default Paths
SMALL_SAMPLE_PATH = os.path.join("data", "orders_small_sample.csv")
HUGE_FILE_PATH = os.path.join("data", "orders_huge_mixed_quality.csv")
REPORTS_DIR = "reports"
RESULTS_FILE = os.path.join(REPORTS_DIR, "results.json")

# Pipeline Settings
BATCH_SIZE = 5000