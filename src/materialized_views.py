"""
materialized_views.py - العروض المادية والتحديث التزايدي الذكي (Phase 2)
========================================================================
متطلبات القسم 3 من وثيقة المشروع النهائي:
1. إنشاء 2 Materialized Views على الأقل:
   - daily_sales_summary: ملخص المبيعات اليومية
   - top_products_summary: ملخص أداء المنتجات
2. آلية تحديث تزايدي (Incremental Refresh) واضحة تستفيد من السجلات الجديدة فقط
   دون إعادة بناء كل البيانات من الصفر في كل مرة.
"""

import json
import time
from datetime import datetime
from collections import defaultdict
from pymongo import MongoClient, UpdateOne
from bson import ObjectId
from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_VALIDATED

# أسماء مجموعات العروض المادية
COLLECTION_MV_DAILY_SALES = "daily_sales_summary"
COLLECTION_MV_TOP_PRODUCTS = "top_products_summary"
COLLECTION_MV_METADATA = "mv_metadata"

def get_db(db=None):
    if db is None:
        client = MongoClient(MONGO_URI)
        return client[MONGO_DB_NAME]
    return db

def get_mv_metadata(db, view_name):
    """استرجاع بيانات آخر تحديث للعرض المادي"""
    return db[COLLECTION_MV_METADATA].find_one({"_id": view_name}) or {}

def set_mv_metadata(db, view_name, last_id, doc_count, refresh_type="incremental"):
    """تسجيل بيانات نقطة التزامن للعرض المادي"""
    db[COLLECTION_MV_METADATA].replace_one(
        {"_id": view_name},
        {
            "_id": view_name,
            "last_synced_id": str(last_id) if last_id else None,
            "total_documents_processed": doc_count,
            "last_refresh_type": refresh_type,
            "last_refreshed_at": datetime.now().isoformat()
        },
        upsert=True
    )

# ==============================================================================
# 1. العرض المادي: ملخص المبيعات اليومية (daily_sales_summary)
# ==============================================================================
def refresh_daily_sales_summary(db=None, incremental=True):
    """
    تحديث العرض المادي للمبيعات اليومية بنمط تزايدي (Incremental)
    - إذا كان incremental=True وسبق التحديث: يعالج فقط السجلات ذات (_id > last_synced_id).
    - يطبق مشغل $inc الذري لإضافة مبالغ وأعداد الطلبات الجديدة إلى اليوم المقابل.
    """
    db = get_db(db)
    valid_col = db[COLLECTION_VALIDATED]
    mv_col = db[COLLECTION_MV_DAILY_SALES]
    
    meta = get_mv_metadata(db, COLLECTION_MV_DAILY_SALES)
    last_synced_id = meta.get("last_synced_id")
    
    query = {}
    refresh_mode = "full"
    
    if incremental and last_synced_id:
        try:
            query = {"_id": {"$gt": ObjectId(last_synced_id)}}
            refresh_mode = "incremental"
        except Exception:
            query = {}
            refresh_mode = "full"
            
    if refresh_mode == "full":
        mv_col.delete_many({})
        
    start_time = time.time()
    
    # استخراج السجلات الجديدة فقط
    pipeline = [
        {"$match": query},
        {"$match": {"order_date": {"$ne": None, "$ne": ""}}},
        {
            "$group": {
                "_id": {"$substr": ["$order_date", 0, 10]}, # YYYY-MM-DD
                "delta_sales": {
                    "$sum": {
                        "$convert": {
                            "input": "$total_amount",
                            "to": "double",
                            "onError": 0.0,
                            "onNull": 0.0
                        }
                    }
                },
                "delta_orders": {"$sum": 1},
                "max_id": {"$max": "$_id"}
            }
        }
    ]
    
    deltas = list(valid_col.aggregate(pipeline))
    if not deltas:
        return {
            "view_name": COLLECTION_MV_DAILY_SALES,
            "status": "UP_TO_DATE",
            "refresh_mode": refresh_mode,
            "days_updated": 0,
            "elapsed_seconds": round(time.time() - start_time, 3)
        }
        
    bulk_ops = []
    overall_max_id = None
    now_str = datetime.now().isoformat()
    
    for d in deltas:
        day = d["_id"]
        delta_sales = round(float(d.get("delta_sales", 0.0)), 2)
        delta_orders = int(d.get("delta_orders", 0))
        m_id = d.get("max_id")
        if overall_max_id is None or (m_id and str(m_id) > str(overall_max_id)):
            overall_max_id = m_id
            
        bulk_ops.append(
            UpdateOne(
                {"_id": day},
                {
                    "$set": {"date": day, "last_refreshed_at": now_str},
                    "$inc": {"total_sales": delta_sales, "order_count": delta_orders}
                },
                upsert=True
            )
        )
        
    if bulk_ops:
        mv_col.bulk_write(bulk_ops, ordered=False)
        
    # تحديث متوسط قيمة الطلب للأيام المعدلة
    for d in deltas:
        day = d["_id"]
        doc = mv_col.find_one({"_id": day})
        if doc and doc.get("order_count", 0) > 0:
            avg_val = round(doc.get("total_sales", 0.0) / doc["order_count"], 2)
            mv_col.update_one({"_id": day}, {"$set": {"avg_order_value": avg_val}})
            
    # تحديث الميتاداتا
    set_mv_metadata(db, COLLECTION_MV_DAILY_SALES, overall_max_id, len(deltas), refresh_mode)
    
    return {
        "view_name": COLLECTION_MV_DAILY_SALES,
        "status": "SUCCESS",
        "refresh_mode": refresh_mode,
        "days_updated": len(deltas),
        "last_synced_id": str(overall_max_id),
        "elapsed_seconds": round(time.time() - start_time, 3)
    }

# ==============================================================================
# 2. العرض المادي: ملخص أفضل المنتجات (top_products_summary)
# ==============================================================================
def refresh_top_products_summary(db=None, incremental=True):
    """
    تحديث العرض المادي لأداء المنتجات بنمط تزايدي (Incremental)
    - يعالج السجلات الجديدة ويضيف الكميات والإيرادات ذرياً عبر $inc
    """
    db = get_db(db)
    valid_col = db[COLLECTION_VALIDATED]
    mv_col = db[COLLECTION_MV_TOP_PRODUCTS]
    
    meta = get_mv_metadata(db, COLLECTION_MV_TOP_PRODUCTS)
    last_synced_id = meta.get("last_synced_id")
    
    query = {"items_json": {"$ne": None}}
    refresh_mode = "full"
    
    if incremental and last_synced_id:
        try:
            query["_id"] = {"$gt": ObjectId(last_synced_id)}
            refresh_mode = "incremental"
        except Exception:
            refresh_mode = "full"
            
    if refresh_mode == "full":
        mv_col.delete_many({})
        
    start_time = time.time()
    
    # قراءة السجلات الجديدة وتحليل عناصرها
    prod_deltas = defaultdict(lambda: {"name": "", "qty": 0, "revenue": 0.0, "orders": 0})
    latest_id = None
    processed_count = 0
    
    # دفعات تدفقية خفيفة
    cursor = valid_col.find(query, {"items_json": 1}).sort("_id", 1).limit(50000)
    for doc in cursor:
        latest_id = doc["_id"]
        processed_count += 1
        items_str = doc.get("items_json")
        if not items_str:
            continue
        try:
            items = json.loads(items_str)
            if isinstance(items, list):
                for it in items:
                    sku = it.get("sku") or "UNKNOWN_SKU"
                    name = it.get("name") or sku
                    qty = int(it.get("qty", 1) or 1)
                    rev = float(it.get("total", 0.0) or 0.0)
                    
                    prod_deltas[sku]["name"] = name
                    prod_deltas[sku]["qty"] += qty
                    prod_deltas[sku]["revenue"] += rev
                    prod_deltas[sku]["orders"] += 1
        except Exception:
            continue
            
    if not prod_deltas:
        return {
            "view_name": COLLECTION_MV_TOP_PRODUCTS,
            "status": "UP_TO_DATE",
            "refresh_mode": refresh_mode,
            "products_updated": 0,
            "elapsed_seconds": round(time.time() - start_time, 3)
        }
        
    bulk_ops = []
    now_str = datetime.now().isoformat()
    
    for sku, d in prod_deltas.items():
        bulk_ops.append(
            UpdateOne(
                {"_id": sku},
                {
                    "$set": {"sku": sku, "product_name": d["name"], "last_refreshed_at": now_str},
                    "$inc": {
                        "total_quantity": d["qty"],
                        "total_revenue": round(d["revenue"], 2),
                        "orders_count": d["orders"]
                    }
                },
                upsert=True
            )
        )
        
    if bulk_ops:
        mv_col.bulk_write(bulk_ops, ordered=False)
        
    set_mv_metadata(db, COLLECTION_MV_TOP_PRODUCTS, latest_id, processed_count, refresh_mode)
    
    return {
        "view_name": COLLECTION_MV_TOP_PRODUCTS,
        "status": "SUCCESS",
        "refresh_mode": refresh_mode,
        "products_updated": len(prod_deltas),
        "records_processed": processed_count,
        "last_synced_id": str(latest_id),
        "elapsed_seconds": round(time.time() - start_time, 3)
    }

# ==============================================================================
# دالة التحديث الموحدة لكافة العروض المادية (لخدمة الـ API والـ Scheduler)
# ==============================================================================
def refresh_all_materialized_views(db=None, incremental=True):
    """
    تحديث كافة العروض المادية في المشروع دفعة واحدة (تزايدياً افتراضياً)
    """
    db = get_db(db)
    res_daily = refresh_daily_sales_summary(db, incremental=incremental)
    res_products = refresh_top_products_summary(db, incremental=incremental)
    
    return {
        "timestamp": datetime.now().isoformat(),
        "incremental": incremental,
        "daily_sales_summary": res_daily,
        "top_products_summary": res_products
    }

def get_materialized_view_data(view_name, db=None, limit=20):
    """استرجاع بيانات عرض مادي محدد مع الفرز التنازلي"""
    db = get_db(db)
    sort_key = "total_sales" if view_name == COLLECTION_MV_DAILY_SALES else "total_revenue"
    docs = list(db[view_name].find({}, {"_id": 0}).sort(sort_key, -1).limit(limit))
    return docs

def get_daily_sales_summary(db=None, limit=20):
    """استرجاع بيانات ملخص المبيعات اليومية"""
    return get_materialized_view_data(COLLECTION_MV_DAILY_SALES, db=db, limit=limit)

def get_top_products_summary(db=None, limit=20):
    """استرجاع بيانات ملخص أفضل المنتجات مبيعاً"""
    return get_materialized_view_data(COLLECTION_MV_TOP_PRODUCTS, db=db, limit=limit)
