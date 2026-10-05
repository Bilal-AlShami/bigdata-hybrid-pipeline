"""
scheduler.py - المهام المجدولة وتسجيل أثر التنفيذ (Phase 2)
===========================================================
متطلبات القسم 4 من وثيقة المشروع النهائي:
1. إنشاء مهمتين مجدولتين على الأقل تؤديان وظائف فعلية مرتبطة بالمشروع:
   - المهمة 1: تحديث العروض المادية دورياً (refresh_materialized_views_job)
   - المهمة 2: إنشاء تقرير دوري لمؤشرات الأداء الرئيسية (periodic_kpi_report_job)
2. جدول زمني محدد مع إمكانية التشغيل اليدوي الفوري أثناء الاختبار والمناقشة.
3. تسجيل نتيجة التنفيذ ووقت البداية والنهاية والمدة وحالة النجاح أو الفشل.
"""

import time
import json
import os
import threading
from datetime import datetime
from pymongo import MongoClient, DESCENDING
from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_VALIDATED
from src.materialized_views import refresh_all_materialized_views, COLLECTION_MV_DAILY_SALES, COLLECTION_MV_TOP_PRODUCTS

COLLECTION_JOB_LOGS = "job_logs"
REPORTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")

def get_db(db=None):
    if db is None:
        client = MongoClient(MONGO_URI)
        return client[MONGO_DB_NAME]
    return db

# ==============================================================================
# وظائف المهام الفعلية (Job Workflows)
# ==============================================================================

def execute_refresh_materialized_views(db=None):
    """
    المهمة 1: التحديث التزايدي للعروض المادية (Materialized Views)
    """
    db = get_db(db)
    res = refresh_all_materialized_views(db, incremental=True)
    summary = (
        f"تم التحديث التزايدي: {res['daily_sales_summary'].get('days_updated', 0)} يوماً "
        f"و {res['top_products_summary'].get('products_updated', 0)} منتجاً"
    )
    return summary, res

def execute_periodic_kpi_report(db=None):
    """
    المهمة 2: إنشاء وحفظ تقرير دوري لمؤشرات الأداء الرئيسية (KPIs)
    """
    db = get_db(db)
    valid_col = db[COLLECTION_VALIDATED]
    
    total_orders = valid_col.count_documents({})
    pipeline_sales = [
        {"$group": {"_id": None, "total": {"$sum": {"$convert": {"input": "$total_amount", "to": "double", "onError": 0.0, "onNull": 0.0}}}}}
    ]
    sales_res = list(valid_col.aggregate(pipeline_sales))
    total_revenue = round(sales_res[0]["total"], 2) if sales_res else 0.0
    
    top_city_doc = list(valid_col.aggregate([
        {"$match": {"city": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$city", "sales": {"$sum": {"$convert": {"input": "$total_amount", "to": "double", "onError": 0.0, "onNull": 0.0}}}}},
        {"$sort": {"sales": -1}},
        {"$limit": 1}
    ]))
    top_city = top_city_doc[0]["_id"] if top_city_doc else "N/A"
    
    kpi_data = {
        "report_timestamp": datetime.now().isoformat(),
        "total_validated_orders": total_orders,
        "total_revenue_yer": total_revenue,
        "avg_order_value_yer": round(total_revenue / total_orders, 2) if total_orders > 0 else 0.0,
        "top_city_by_sales": top_city,
        "materialized_views_status": "ONLINE"
    }
    
    # حفظ التقرير في ملف JSON داخل reports/
    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_file = os.path.join(REPORTS_DIR, "periodic_kpi_summary.json")
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(kpi_data, f, ensure_ascii=False, indent=2)
        
    summary = f"تم استخراج مؤشرات الأداء: {total_orders:,} طلباً بإجمالي مبيعات {total_revenue:,.2f} ريال"
    return summary, kpi_data

# ==============================================================================
# سجل المهام وجدولة التشغيل (Registry & Execution Logger)
# ==============================================================================

REGISTERED_JOBS = {
    "refresh_materialized_views_job": {
        "title": "تحديث العروض المادية التزايدي",
        "description": "تحديث مجموعتي daily_sales_summary و top_products_summary دورياً",
        "interval_seconds": 900, # كل 15 دقيقة
        "func": execute_refresh_materialized_views,
        "last_run": None,
        "last_status": "IDLE"
    },
    "periodic_kpi_report_job": {
        "title": "إنشاء تقرير مؤشرات الأداء الدوري",
        "description": "استخراج ملخص المبيعات والطلبات وأفضل المدن وحفظه في reports/periodic_kpi_summary.json",
        "interval_seconds": 1800, # كل 30 دقيقة
        "func": execute_periodic_kpi_report,
        "last_run": None,
        "last_status": "IDLE"
    }
}

def log_job_execution(db, job_name, trigger_type, started_at, finished_at, duration_ms, status, summary, details=None, error=None):
    """تسجيل نتيجة تنفيذ المهمة في قاعدة البيانات"""
    log_doc = {
        "job_name": job_name,
        "trigger_type": trigger_type, # MANUAL أو SCHEDULED
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_ms": duration_ms,
        "status": status, # SUCCESS أو FAILED
        "summary": summary,
        "details": details,
        "error": str(error) if error else None
    }
    db[COLLECTION_JOB_LOGS].insert_one(log_doc)
    return log_doc

def run_job(job_name, trigger_type="MANUAL", db=None):
    """
    تشغيل مهمة محددة (يدوياً أو آلياً) وتسجيل كامل تفاصيل البداية والنهاية والحالة
    """
    if job_name not in REGISTERED_JOBS:
        raise ValueError(f"المهمة '{job_name}' غير مسجلة. المهام المتاحة: {list(REGISTERED_JOBS.keys())}")
        
    db = get_db(db)
    job_info = REGISTERED_JOBS[job_name]
    
    started_at = datetime.now()
    t0 = time.time()
    
    try:
        summary, details = job_info["func"](db)
        finished_at = datetime.now()
        duration_ms = round((time.time() - t0) * 1000, 2)
        status = "SUCCESS"
        error = None
        job_info["last_status"] = "SUCCESS"
    except Exception as e:
        finished_at = datetime.now()
        duration_ms = round((time.time() - t0) * 1000, 2)
        status = "FAILED"
        summary = f"فشل تنفيذ المهمة: {e}"
        details = None
        error = str(e)
        job_info["last_status"] = "FAILED"
        
    job_info["last_run"] = finished_at.isoformat()
    
    log_entry = log_job_execution(
        db=db,
        job_name=job_name,
        trigger_type=trigger_type,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        status=status,
        summary=summary,
        details=details,
        error=error
    )
    
    # حذف _id من القاموس لدعم تحويله إلى JSON
    log_entry.pop("_id", None)
    return log_entry

def get_job_logs(job_name=None, limit=20, db=None):
    """استرجاع سجلات تنفيذ المهام مرتبة من الأحدث للأقدم"""
    db = get_db(db)
    query = {"job_name": job_name} if job_name else {}
    logs = list(db[COLLECTION_JOB_LOGS].find(query, {"_id": 0}).sort("started_at", DESCENDING).limit(limit))
    return logs

# ==============================================================================
# محرك الجدولة في الخلفية (Background Scheduler Thread)
# ==============================================================================

class BackgroundJobScheduler:
    """
    مجدول مهام خفيف يعمل في خيط منفصل (Background Thread)
    - متوافق 100% مع كافة البيئات دون الحاجة لمكتبات C خارجية.
    - يدعم التشغيل الآلي وفق الفترات الزمنية المحددة.
    """
    def __init__(self):
        self._running = False
        self._thread = None
        self._last_ticks = {name: time.time() for name in REGISTERED_JOBS}
        
    def _run_loop(self):
        while self._running:
            now = time.time()
            for name, job_info in REGISTERED_JOBS.items():
                interval = job_info["interval_seconds"]
                last_tick = self._last_ticks.get(name, 0)
                if now - last_tick >= interval:
                    try:
                        run_job(name, trigger_type="SCHEDULED")
                    except Exception:
                        pass
                    self._last_ticks[name] = now
            time.sleep(5)
            
    def start(self):
        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="JobSchedulerThread")
            self._thread.start()
            
    def stop(self):
        self._running = False

# كائن المجدول العام المشترك
global_scheduler = BackgroundJobScheduler()
