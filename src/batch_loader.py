import csv
import time
import sys
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
    except Exception:
        pass
from datetime import datetime
from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_RAW, BATCH_SIZE

def load_batch(run_id, file_path, batch_size=None):
    effective_batch_size = batch_size or BATCH_SIZE
    print(f"[{run_id}] ⚡ بدء تحميل Python Batch (حجم الدفعة: {effective_batch_size:,})...")
    
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    db = client[MONGO_DB_NAME]
    collection = db[COLLECTION_RAW]
    
    batch = []
    total_inserted = 0
    batch_num = 1
    row_num = 2  # Assuming 1 is header
    
    start_time = time.time()
    
    try:
        with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
            reader = csv.DictReader(f)
            for row in reader:
                doc = {
                    "run_id": run_id,
                    "source_file": file_path,
                    "source_row_number": row_num,
                    "ingested_at": datetime.now().isoformat(),
                    "engine_used": "python_batch",
                    "raw_record": row
                }
                batch.append(doc)
                
                if len(batch) >= effective_batch_size:
                    try:
                        collection.insert_many(batch)
                        total_inserted += len(batch)
                        elapsed = time.time() - start_time
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        print(f"Batch {batch_num}: Inserted {total_inserted} | Time: {elapsed:.2f}s | Rate: {rate:.2f} rows/s")
                    except Exception as batch_e:
                        print(f"Error inserting batch {batch_num}: {batch_e}")
                        
                    batch = []
                    batch_num += 1
                row_num += 1
                
            # Insert remaining
            if batch:
                try:
                    collection.insert_many(batch)
                    total_inserted += len(batch)
                    elapsed = time.time() - start_time
                    rate = total_inserted / elapsed if elapsed > 0 else 0
                    print(f"Final Batch {batch_num}: Inserted {total_inserted} | Time: {elapsed:.2f}s | Rate: {rate:.2f} rows/s")
                except Exception as batch_e:
                    print(f"Error inserting final batch: {batch_e}")
                    
        return total_inserted
        
    except Exception as e:
        print(f"حدث خطأ عام أثناء التحميل الدفعي: {e}")
        return 0
    finally:
        client.close()