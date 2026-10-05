"""
api.py - واجهة الـ API الموحدة للتشغيل والاختبار عبر FastAPI (Phase 2)
====================================================================
متطلبات القسم 5 من وثيقة المشروع النهائي:
1. توفير واجهة API موحدة باستخدام FastAPI تتيح لنظام التقييم تشغيل واختبار وظائف المشروع.
2. استجابات بصيغة JSON مع توفير صفحة Swagger التفاعلية عبر docs/.
3. المسارات الـ 10 الإلزامية:
   - GET  /health
   - POST /ingest
   - POST /indexes
   - GET  /queries
   - GET  /queries/{name}
   - GET  /aggregations
   - GET  /aggregations/{name}
   - POST /refresh-mv
   - GET  /jobs
   - POST /jobs/{name}/run
"""

import os
import uuid
import time
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from pymongo import MongoClient

from config.settings import MONGO_URI, MONGO_DB_NAME
from src.file_router import select_processing_engine
from src.batch_loader import load_batch
from src.elt_pipeline import run_elt
from src.metrics import generate_metrics
from src.indexes_queries import (
    create_project_indexes,
    run_explain_analysis,
    REGISTERED_QUERIES
)
from src.aggregations import (
    REGISTERED_AGGREGATIONS,
    get_db
)
from src.materialized_views import (
    refresh_all_materialized_views,
    get_materialized_view_data,
    COLLECTION_MV_DAILY_SALES,
    COLLECTION_MV_TOP_PRODUCTS
)
from src.scheduler import (
    REGISTERED_JOBS,
    run_job,
    get_job_logs,
    global_scheduler
)

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    global_scheduler.start()
    yield
    global_scheduler.stop()

# تهيئة تطبيق FastAPI مع عنوان وتوثيق أكاديمي
app = FastAPI(
    title="🚀 Big Data Hybrid Pipeline - Unified Testing & Evaluation API",
    description="واجهة التشغيل والتقييم الموحدة للمشروع النهائي لمقرر البيانات الضخمة (جامعة الرازي - م. عمر أبوسند)",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# ربط مجلد الملفات الثابتة ولوحة التحكم الرسومية (Glassmorphism Dashboard)
STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")
if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ==============================================================================
# 0. Root Dashboard & Favicon Handlers
# ==============================================================================
@app.get("/", include_in_schema=False)
def root_dashboard():
    """عرض لوحة التحكم التفاعلية فائقة الجمال (Glassmorphism Dashboard)"""
    index_file = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return RedirectResponse(url="/docs")

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(content=b"", media_type="image/x-icon")

# ==============================================================================
# 1. GET /health (and /api/health)
# ==============================================================================
@app.get("/health", tags=["System Health"])
@app.get("/api/health", tags=["System Health"], include_in_schema=False)
def health_check():
    """فحص سلامة النظام والاتصال بقاعدة بيانات MongoDB"""
    db_status = "DISCONNECTED"
    latency_ms = None
    try:
        t0 = time.time()
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        latency_ms = round((time.time() - t0) * 1000, 2)
        db_status = "CONNECTED"
    except Exception as e:
        db_status = f"ERROR: {e}"
        
    return {
        "status": "UP",
        "service": "Big Data Pipeline Unified API (FastAPI)",
        "database": db_status,
        "database_latency_ms": latency_ms,
        "timestamp": datetime.now().isoformat(),
        "scheduler_status": "RUNNING"
    }

@app.get("/metrics", tags=["System Health"])
@app.get("/api/metrics", tags=["System Health"], include_in_schema=False)
def get_metrics():
    """استرجاع أحدث مؤشرات الأداء المحفوظة من نتائج معالجة خط الأنابيب"""
    raw_c, valid_c, quar_c = 0, 0, 0
    try:
        db = get_db()
        raw_c = db["orders_raw"].estimated_document_count()
        valid_c = db["orders_validated"].estimated_document_count()
        quar_c = db["orders_quarantine"].estimated_document_count()
    except Exception:
        pass
    
    metrics_file = "reports/results.json"
    cached = {}
    if os.path.exists(metrics_file):
        try:
            with open(metrics_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
        except Exception:
            pass
            
    return {
        "orders_raw_count": raw_c,
        "orders_validated_count": valid_c,
        "orders_quarantine_count": quar_c,
        "consistency_equation_status": "VALID" if raw_c >= (valid_c + quar_c) else "CHECK_REQUIRED",
        "latest_pipeline_run": cached
    }

# ==============================================================================
# 2. POST /ingest
# ==============================================================================
class IngestRequest(BaseModel):
    file_path: Optional[str] = Field("data/01_student_test_small.csv", description="مسار الملف المراد إدخاله ومعالجته")
    batch_size: Optional[int] = Field(2500, description="حجم الدفعة لمحرك ELT")

@app.post("/ingest", tags=["Pipeline Execution"])
def trigger_ingest(payload: IngestRequest):
    """
    تشغيل بوابة الإدخال وخط الأنابيب الهجين الكامل:
    1. فحص حجم الملف وتوجيهه تلقائياً عبر File Router.
    2. التحميل الخام (Raw Ingestion).
    3. تطبيق قواعد التنظيف والعزل والـ Upsert عبر ELT.
    4. حساب وتوثيق المقاييس ومعادلة الاتساق.
    """
    file_path = payload.file_path
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"ملف الإدخال غير موجود: {file_path}")
        
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    start_total = time.time()
    
    # 1. توجيه المحرك
    engine = select_processing_engine(file_path)
    
    # 2. التحميل الخام
    raw_loaded = 0
    if engine == "python_batch":
        raw_loaded = load_batch(run_id, file_path)
    elif engine == "pyspark":
        try:
            from src.spark_loader import load_spark
            raw_loaded = load_spark(run_id, file_path)
        except ImportError:
            raise HTTPException(status_code=500, detail="PySpark غير مثبت في هذه البيئة، يرجى تشغيل Python 3.12")
        
    # 3. التحويل والتنظيف والـ Upsert
    elt_counters, elt_elapsed = run_elt(run_id, batch_size=payload.batch_size)
    
    # 4. توليد المقاييس وحفظها
    total_elapsed = round(time.time() - start_total, 2)
    metrics_record = generate_metrics(
        run_id,
        file_path,
        engine,
        raw_loaded,
        elt_counters,
        total_seconds=total_elapsed
    )
    
    return {
        "status": "SUCCESS",
        "run_id": run_id,
        "engine_used": engine,
        "file_path": file_path,
        "raw_loaded": raw_loaded,
        "elt_counters": elt_counters,
        "consistency_check": metrics_record.get("consistency_check", "PASS"),
        "total_elapsed_seconds": total_elapsed
    }

# ==============================================================================
# 3. POST /indexes
# ==============================================================================
@app.post("/indexes", tags=["Indexes & Performance"])
def create_indexes_endpoint(run_explain: bool = Query(True, description="تنفيذ تحليل Explain بعد إنشاء الفهارس")):
    """
    إنشاء كافة الفهارس المطلوبة (بما فيها Compound Index)
    مع إمكانية تنفيذ تحليل explain('executionStats') لـ 3 استعلامات وإرجاع مصفوفة توثيق الأثر
    """
    db = get_db()
    created = create_project_indexes(db)
    
    explain_results = []
    if run_explain:
        explain_results = run_explain_analysis(db)
        
    return {
        "status": "SUCCESS",
        "indexes_created_count": len(created),
        "indexes": created,
        "explain_analysis": explain_results
    }

# ==============================================================================
# 4. GET /queries & GET /queries/{name}
# ==============================================================================
@app.get("/queries", tags=["Queries"])
def list_queries():
    """استرجاع قائمة الاستعلامات الخمسة المتاحة مع وصفها ومعاملاتها الافتراضية"""
    queries_meta = {}
    for name, q in REGISTERED_QUERIES.items():
        queries_meta[name] = {
            "title": q["title"],
            "description": q["description"],
            "default_params": q["default_params"]
        }
    return {
        "count": len(queries_meta),
        "queries": queries_meta
    }

@app.get("/queries/{name}", tags=["Queries"])
def execute_query(
    name: str,
    customer_id: Optional[str] = Query(None, description="معرف العميل (لاستعلام customer_orders)"),
    start_date: Optional[str] = Query("2025-01-01", description="تاريخ البداية YYYY-MM-DD"),
    end_date: Optional[str] = Query("2025-12-31", description="تاريخ النهاية YYYY-MM-DD"),
    status: Optional[str] = Query("Completed", description="حالة الطلب"),
    min_amount: Optional[float] = Query(10000.0, description="الحد الأدنى للقيمة المالية"),
    city: Optional[str] = Query("صنعاء", description="اسم المدينة"),
    delivery_type: Optional[str] = Query("Express", description="نوع التوصيل"),
    payment_method: Optional[str] = Query("Cash", description="طريقة الدفع"),
    payment_status: Optional[str] = Query("Paid", description="حالة السداد"),
    limit: Optional[int] = Query(20, ge=1, le=500, description="عدد السجلات الأقصى")
):
    """تنفيذ استعلام محدد بالاسم واسترجاع السجلات المطابقة بصيغة JSON"""
    if name not in REGISTERED_QUERIES:
        raise HTTPException(
            status_code=404,
            detail=f"الاستعلام '{name}' غير موجود. الاستعلامات المتاحة: {list(REGISTERED_QUERIES.keys())}"
        )
        
    db = get_db()
    func = REGISTERED_QUERIES[name]["func"]
    
    # توجيه المعاملات بحسب نوع الاستعلام
    if name == "customer_orders":
        cid = customer_id or REGISTERED_QUERIES[name]["default_params"]["customer_id"]
        results = func(db, customer_id=cid, limit=limit)
    elif name == "orders_by_date_and_status":
        results = func(db, start_date=start_date, end_date=end_date, status=status, limit=limit)
    elif name == "high_value_orders":
        results = func(db, min_amount=min_amount, limit=limit)
    elif name == "city_delivery_orders":
        results = func(db, city=city, delivery_type=delivery_type, limit=limit)
    elif name == "orders_by_payment":
        results = func(db, payment_method=payment_method, payment_status=payment_status, limit=limit)
    else:
        results = []
        
    return {
        "query_name": name,
        "title": REGISTERED_QUERIES[name]["title"],
        "records_count": len(results),
        "results": results
    }

# ==============================================================================
# 5. GET /aggregations & GET /aggregations/{name}
# ==============================================================================
@app.get("/aggregations", tags=["Aggregations"])
def list_aggregations():
    """استرجاع قائمة تقارير التجميعات الخمسة المتاحة"""
    aggs_meta = {}
    for name, agg in REGISTERED_AGGREGATIONS.items():
        aggs_meta[name] = {
            "title": agg["title"],
            "description": agg["description"],
            "default_params": agg["default_params"]
        }
    return {
        "count": len(aggs_meta),
        "aggregations": aggs_meta
    }

@app.get("/aggregations/{name}", tags=["Aggregations"])
def execute_aggregation(
    name: str,
    limit: Optional[int] = Query(20, ge=1, le=200, description="الحد الأقصى للنتائج")
):
    """تنفيذ تقرير تجميعي محدد بالاسم واسترجاع البيانات التحليلية"""
    if name not in REGISTERED_AGGREGATIONS:
        raise HTTPException(
            status_code=404,
            detail=f"التقرير '{name}' غير موجود. التقارير المتاحة: {list(REGISTERED_AGGREGATIONS.keys())}"
        )
        
    db = get_db()
    func = REGISTERED_AGGREGATIONS[name]["func"]
    
    if name == "orders_by_status":
        results = func(db)
    else:
        results = func(db, limit=limit)
        
    return {
        "aggregation_name": name,
        "title": REGISTERED_AGGREGATIONS[name]["title"],
        "records_count": len(results),
        "data": results
    }

# ==============================================================================
# 6. Materialized Views: POST /refresh-mv, GET /views/daily-sales, GET /views/top-products
# ==============================================================================
@app.post("/refresh-mv", tags=["Materialized Views"])
@app.post("/api/views/refresh", tags=["Materialized Views"], include_in_schema=False)
def refresh_materialized_views_endpoint(
    incremental: bool = Query(True, description="تفعيل التحديث التزايدي (Incremental) دون مسح البيانات السابقة")
):
    """
    تحديث العروض المادية (daily_sales_summary و top_products_summary)
    يدعم التحديث التزايدي الذكي (Incremental Refresh) عبر $inc والسجلات الجديدة فقط
    """
    db = get_db()
    res = refresh_all_materialized_views(db, incremental=incremental)
    
    return {
        "status": "SUCCESS",
        "message": "تم تحديث العروض المادية بنجاح",
        "details": res
    }

@app.get("/views/daily-sales", tags=["Materialized Views"])
@app.get("/api/views/daily-sales", tags=["Materialized Views"], include_in_schema=False)
def get_daily_sales_view(limit: int = Query(20, ge=1, le=200, description="الحد الأقصى للنتائج")):
    """استرجاع بيانات ملخص المبيعات اليومية من الجدول المادي daily_sales_summary"""
    db = get_db()
    data = get_materialized_view_data(COLLECTION_MV_DAILY_SALES, db=db, limit=limit)
    return {
        "view_name": COLLECTION_MV_DAILY_SALES,
        "count": len(data),
        "data": data
    }

@app.get("/views/top-products", tags=["Materialized Views"])
@app.get("/api/views/top-products", tags=["Materialized Views"], include_in_schema=False)
def get_top_products_view(limit: int = Query(20, ge=1, le=200, description="الحد الأقصى للنتائج")):
    """استرجاع بيانات ملخص أفضل المنتجات من الجدول المادي top_products_summary"""
    db = get_db()
    data = get_materialized_view_data(COLLECTION_MV_TOP_PRODUCTS, db=db, limit=limit)
    return {
        "view_name": COLLECTION_MV_TOP_PRODUCTS,
        "count": len(data),
        "data": data
    }

# ==============================================================================
# 7. GET /jobs & POST /jobs/{name}/run (Scheduled Jobs)
# ==============================================================================
@app.get("/jobs", tags=["Scheduled Jobs"])
def list_jobs():
    """استرجاع قائمة المهام المجدولة المسجلة مع آخر سجلات التنفيذ في قاعدة البيانات"""
    jobs_info = {}
    for name, j in REGISTERED_JOBS.items():
        jobs_info[name] = {
            "title": j["title"],
            "description": j["description"],
            "interval_seconds": j["interval_seconds"],
            "last_run": j["last_run"],
            "last_status": j["last_status"]
        }
        
    logs = get_job_logs(limit=10)
    return {
        "registered_jobs": jobs_info,
        "recent_execution_logs": logs
    }

@app.post("/jobs/{name}/run", tags=["Scheduled Jobs"])
def run_job_manually_endpoint(name: str):
    """
    تشغيل يدوي فوري لمهمة محددة أثناء الاختبار أو المناقشة
    وتسجيل وقت البداية والنهاية والمدة والحالة في مجموعة job_logs
    """
    if name not in REGISTERED_JOBS:
        raise HTTPException(
            status_code=404,
            detail=f"المهمة '{name}' غير موجودة. المهام المتاحة: {list(REGISTERED_JOBS.keys())}"
        )
        
    db = get_db()
    execution_result = run_job(name, trigger_type="MANUAL", db=db)
    
    return {
        "status": "SUCCESS",
        "message": f"تم تنفيذ المهمة '{name}' يدوياً بنجاح",
        "execution_log": execution_result
    }
