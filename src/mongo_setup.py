import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
    except Exception:
        pass

from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_VALIDATED, COLLECTION_RAW, COLLECTION_QUARANTINE

def setup_mongodb():
    """
    تهيئة مجموعات MongoDB والفهارس وقواعد التحقق من المخطط (Schema Validation)
    وفق معايير القسم 6.9 و 6.10:
    - orders_raw: لا يوجد Validator أو Unique Index لمنع تحميل أي بيانات واردة.
    - orders_validated: تطبيق Schema Validation + Unique Index على order_id لدعم Idempotent Upsert.
    - orders_quarantine: حفظ أسباب العزل والسجل الخام.
    """
    print("⚙️ إعداد قاعدة بيانات MongoDB وتطبيق معايير القسم 6.9...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB_NAME]
    
    # 1. التأكد من وجود المجموعات الثلاث
    existing_cols = db.list_collection_names()
    for col_name in [COLLECTION_RAW, COLLECTION_VALIDATED, COLLECTION_QUARANTINE]:
        if col_name not in existing_cols:
            db.create_collection(col_name)
            print(f"-> تم إنشاء المجموعة: {col_name}")

    # 2. إنشاء Unique Index على order_id في مجموعة orders_validated لضمان الـ Idempotency
    print("-> إنشاء Unique Index على حقل order_id في مجموعة orders_validated...")
    db[COLLECTION_VALIDATED].create_index("order_id", unique=True)
    
    # 3. تطبيق Schema Validation على مجموعة orders_validated وفق القسم 6.9
    validation_schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["order_id", "order_date", "status", "customer_id", "quality_status"],
            "properties": {
                "order_id": {
                    "bsonType": "string",
                    "description": "معرف الطلب التجاري - إلزامي وفريد"
                },
                "order_date": {
                    "bsonType": "string",
                    "description": "تاريخ الطلب الموحد"
                },
                "status": {
                    "bsonType": "string",
                    "description": "حالة الطلب"
                },
                "customer_id": {
                    "bsonType": "string",
                    "description": "معرف العميل"
                },
                "quality_status": {
                    "enum": ["valid", "corrected"],
                    "description": "حالة الجودة للسجل المقبول"
                }
            }
        }
    }
    
    try:
        db.command({
            "collMod": COLLECTION_VALIDATED,
            "validator": validation_schema,
            "validationLevel": "moderate",
            "validationAction": "warn"
        })
        print("-> ✅ تم تفعيل Schema Validation بنجاح على مجموعة orders_validated.")
    except Exception as e:
        print(f"-> تنبيه أثناء إعداد Schema Validation: {e}")
    
    print("✅ اكتمل إعداد قاعدة البيانات بنجاح.\n")
    client.close()

if __name__ == "__main__":
    setup_mongodb()
