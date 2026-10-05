import os
from config.settings import SMALL_FILE_THRESHOLD_MB

def select_processing_engine(file_path):
    try:
        file_size_bytes = os.path.getsize(file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        print(f"حجم الملف: {file_size_mb:.2f} MB")

        if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
            print(f"القرار: استخدام محرك (python_batch) لأن الحجم أصغر من أو يساوي {SMALL_FILE_THRESHOLD_MB}MB")
            return "python_batch"
        else:
            print(f"القرار: استخدام محرك (pyspark) لأن الحجم أكبر من {SMALL_FILE_THRESHOLD_MB}MB")
            return "pyspark"

    except FileNotFoundError:
        print(f"خطأ: لم يتم العثور على الملف في المسار: {file_path}")
        return None