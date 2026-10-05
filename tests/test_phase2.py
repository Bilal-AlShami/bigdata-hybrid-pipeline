"""
test_phase2.py - اختبارات المشروع النهائي الشاملة (Big Data Phase 2)
===================================================================
تغطي هذه الاختبارات المتطلبات الإضافية الـ 7 درجات:
1. الفهارس والاستعلامات وتحليل Explain
2. تقارير التجميعات الـ 5 (Aggregations)
3. العروض المادية والتحديث التزايدي (Materialized Views)
4. المهام المجدولة وتسجيل التنفيذ (Scheduled Jobs)
5. واجهة API الموحدة لكافة المسارات العشرة (FastAPI)
"""

import pytest
from fastapi.testclient import TestClient
from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DB_NAME
from src.indexes_queries import create_project_indexes, run_explain_analysis, REGISTERED_QUERIES
from src.aggregations import REGISTERED_AGGREGATIONS
from src.materialized_views import refresh_all_materialized_views, get_materialized_view_data, COLLECTION_MV_DAILY_SALES, COLLECTION_MV_TOP_PRODUCTS
from src.scheduler import REGISTERED_JOBS, run_job, get_job_logs
from src.api import app

@pytest.fixture(scope="module")
def db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB_NAME]

@pytest.fixture(scope="module")
def client():
    return TestClient(app)

# ==============================================================================
# 1. اختبارات الفهارس والاستعلامات (1.5 درجة)
# ==============================================================================
def test_create_indexes(db):
    """التحقق من إنشاء 3 فهارس على الأقل مع وجود Compound Index"""
    indexes = create_project_indexes(db)
    assert len(indexes) >= 3, "يجب إنشاء 3 فهارس على الأقل"
    has_compound = any(idx["type"] == "Compound Index" for idx in indexes)
    assert has_compound, "يجب توفر فهرس مركب (Compound Index) واحد على الأقل"

def test_five_queries_registered():
    """التحقق من تسجيل وتوفر 5 استعلامات عملية"""
    assert len(REGISTERED_QUERIES) >= 5, "يجب توفر 5 استعلامات عملية مسجلة"
    for name, q in REGISTERED_QUERIES.items():
        assert callable(q["func"]), f"دالة الاستعلام {name} يجب أن تكون قابلة للاستدعاء"

def test_explain_analysis(db):
    """التحقق من تنفيذ تحليل Explain لـ 3 استعلامات وتوثيق الأثر"""
    explain_res = run_explain_analysis(db)
    assert len(explain_res) >= 3, "يجب تنفيذ Explain لـ 3 استعلامات على الأقل"
    for sc in explain_res:
        assert "before_indexing" in sc
        assert "after_indexing" in sc
        assert "justification" in sc
        assert "performance_gain" in sc

# ==============================================================================
# 2. اختبارات التجميعات الخمسة (1.5 درجة)
# ==============================================================================
def test_five_aggregations_registered(db):
    """التحقق من وجود 5 تقارير Aggregation تعمل بنجاح"""
    assert len(REGISTERED_AGGREGATIONS) >= 5, "يجب توفر 5 تقارير تجميعية"
    for name, agg in REGISTERED_AGGREGATIONS.items():
        assert callable(agg["func"])
        res = agg["func"](db, **agg["default_params"])
        assert isinstance(res, list), f"التقرير {name} يجب أن يعيد قائمة نتائج"

# ==============================================================================
# 3. اختبارات العروض المادية والتحديث التزايدي (1.5 درجة)
# ==============================================================================
def test_materialized_views_refresh(db):
    """التحقق من بناء وتحديث العرضين الماديين والتحديث التزايدي"""
    # 1. تحديث شامل
    res_full = refresh_all_materialized_views(db, incremental=False)
    assert res_full["daily_sales_summary"]["status"] == "SUCCESS"
    assert res_full["top_products_summary"]["status"] == "SUCCESS"
    
    # 2. تحديث تزايدي
    res_inc = refresh_all_materialized_views(db, incremental=True)
    assert res_inc["daily_sales_summary"]["status"] in ["SUCCESS", "UP_TO_DATE"]
    assert res_inc["top_products_summary"]["status"] in ["SUCCESS", "UP_TO_DATE"]

def test_materialized_views_data(db):
    """التحقق من وجود بيانات صحيحة داخل مجموعات العروض المادية"""
    daily = get_materialized_view_data(COLLECTION_MV_DAILY_SALES, db=db, limit=5)
    products = get_materialized_view_data(COLLECTION_MV_TOP_PRODUCTS, db=db, limit=5)
    assert len(daily) > 0, "مجموعة daily_sales_summary يجب أن تحتوي على سجلات"
    assert len(products) > 0, "مجموعة top_products_summary يجب أن تحتوي على سجلات"

# ==============================================================================
# 4. اختبارات المهام المجدولة (1.0 درجة)
# ==============================================================================
def test_scheduled_jobs(db):
    """التحقق من توفر مهمتين مجدولتين وإمكانية تشغيلهما يدوياً وتسجيل النتيجة"""
    assert len(REGISTERED_JOBS) >= 2, "يجب توفر مهمتين مجدولتين على الأقل"
    
    for job_name in REGISTERED_JOBS:
        log_res = run_job(job_name, trigger_type="MANUAL", db=db)
        assert log_res["status"] == "SUCCESS"
        assert "started_at" in log_res
        assert "finished_at" in log_res
        assert "duration_ms" in log_res
        
    logs = get_job_logs(db=db, limit=5)
    assert len(logs) >= 2, "يجب توفر سجلات تنفيذ للمهام في قاعدة البيانات"

# ==============================================================================
# 5. اختبارات واجهة API الموحدة لكافة المسارات العشرة (0.75 درجة)
# ==============================================================================
def test_api_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "UP"

def test_api_indexes(client):
    r = client.post("/indexes?run_explain=false")
    assert r.status_code == 200
    assert r.json()["indexes_created_count"] >= 3

def test_api_queries_list_and_exec(client):
    # قائمة الاستعلامات
    r1 = client.get("/queries")
    assert r1.status_code == 200
    assert r1.json()["count"] >= 5
    
    # تنفيذ استعلام محدد
    r2 = client.get("/queries/high_value_orders?min_amount=10000&limit=5")
    assert r2.status_code == 200
    assert "results" in r2.json()

def test_api_aggregations_list_and_exec(client):
    # قائمة التجميعات
    r1 = client.get("/aggregations")
    assert r1.status_code == 200
    assert r1.json()["count"] >= 5
    
    # تنفيذ تجميع محدد
    r2 = client.get("/aggregations/sales_by_city?limit=5")
    assert r2.status_code == 200
    assert "data" in r2.json()

def test_api_refresh_mv(client):
    r = client.post("/refresh-mv?incremental=true")
    assert r.status_code == 200
    assert r.json()["status"] == "SUCCESS"

def test_api_jobs_list_and_manual_run(client):
    r1 = client.get("/jobs")
    assert r1.status_code == 200
    assert len(r1.json()["registered_jobs"]) >= 2
    
    r2 = client.post("/jobs/periodic_kpi_report_job/run")
    assert r2.status_code == 200
    assert r2.json()["status"] == "SUCCESS"
