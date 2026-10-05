import time
import uuid
import argparse
import sys
from src.mongo_setup import setup_mongodb
from src.file_router import select_processing_engine
from src.batch_loader import load_batch
from src.spark_loader import load_spark
from src.elt_pipeline import run_elt
from src.metrics import generate_metrics

from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_RAW, COLLECTION_VALIDATED, COLLECTION_QUARANTINE

def main():
    parser = argparse.ArgumentParser(description="Midterm Data Pipeline - Enterprise Hybrid ELT")
    parser.add_argument('--file', type=str, default='data/01_student_test_small.csv', help="Path to the data file to process (default: data/01_student_test_small.csv)")
    parser.add_argument('--batch-size', type=int, default=None, help="Batch size for loading and ELT processing (e.g. 5000, 10000)")
    parser.add_argument('--reset', action='store_true', help="Clean reset: clear raw, validated, quarantine collections and checkpoints before running")
    parser.add_argument('--skip-raw', action='store_true', help="Skip raw loading step and run ELT directly on existing raw data")
    args = parser.parse_args()
    
    file_path = args.file
    batch_size = args.batch_size
    
    print("========================================")
    print("🚀 بدء خط أنابيب البيانات (Hybrid Pipeline)")
    print("========================================")
    
    # تفريغ المجموعات في حال طلب --reset
    if args.reset:
        print("🧹 جاري تفريغ المجموعات وإعادة التعيين (Clean Reset)...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB_NAME]
        db[COLLECTION_RAW].delete_many({})
        db[COLLECTION_VALIDATED].delete_many({})
        db[COLLECTION_QUARANTINE].delete_many({})
        if "checkpoints" in db.list_collection_names():
            db["checkpoints"].delete_many({})
        print("✅ تم تفريغ المجموعات ونقاط الحفظ بنجاح.")
    
    # 1. إعداد قاعدة البيانات
    setup_mongodb()
    
    # 2. إنشاء Run ID
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    print(f"-> Run ID: {run_id}")
    
    # 3. التوجيه (File Router)
    engine = select_processing_engine(file_path)
    if not engine:
        sys.exit(1)
        
    start_time_total = time.time()
    
    # 4. التحميل الخام (Raw Load)
    raw_loaded = 0
    if not args.skip_raw:
        if engine == "python_batch":
            raw_loaded = load_batch(run_id, file_path, batch_size=batch_size)
        else:
            raw_loaded = load_spark(run_id, file_path)
            
        if raw_loaded == 0:
            print("❌ لم يتم تحميل أي بيانات خام، إيقاف الخط.")
            sys.exit(1)
    else:
        print("⏩ تم تفعيل --skip-raw: تخطي مرحلة التحميل الخام.")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        raw_col = client[MONGO_DB_NAME][COLLECTION_RAW]
        raw_loaded = raw_col.count_documents({})
        print(f"-> عدد السجلات الموجودة مسبقاً في {COLLECTION_RAW}: {raw_loaded:,}")
        if raw_loaded == 0:
            print("❌ لا توجد بيانات في orders_raw للبدء منها!")
            sys.exit(1)
        latest_doc = raw_col.find_one(sort=[("_id", -1)])
        if latest_doc and "run_id" in latest_doc:
            run_id = latest_doc["run_id"]
            print(f"-> استخدام Run ID المسجل في البيانات الخام: {run_id}")
            if "checkpoints" in client[MONGO_DB_NAME].list_collection_names():
                client[MONGO_DB_NAME]["checkpoints"].delete_many({"run_id": run_id})
        
    # 5. عملية ELT (التنظيف، التصنيف، التحميل الآمن)
    elt_counters, elt_elapsed = run_elt(run_id, batch_size=batch_size or 5000)
    
    # 6. استخراج المقاييس وحفظها
    total_seconds = time.time() - start_time_total
    generate_metrics(run_id, file_path, engine, raw_loaded, elt_counters, total_seconds)
    
    print("\n✅ اكتمل التشغيل بنجاح.")

if __name__ == "__main__":
    main()