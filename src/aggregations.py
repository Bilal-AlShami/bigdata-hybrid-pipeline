"""
aggregations.py - تقارير التجميعات التحليلية المتقدمة (Phase 2)
============================================================
متطلبات القسم 2 من وثيقة المشروع النهائي:
1. إنشاء 5 تقارير Aggregation على الأقل مناسبة لبيانات التجارة الإلكترونية.
2. لكل تقرير اسم واضح وطريقة تشغيل مستقلة ويعيد نتيجة فعلية.
التقارير الخمسة:
1. المبيعات حسب المدينة (sales_by_city)
2. أفضل المنتجات مبيعاً وإيراداً (top_products)
3. أفضل العملاء إنفاقاً (top_customers)
4. المبيعات عبر الفترات الزمنية (sales_by_period)
5. توزيع الطلبات حسب الحالة (orders_by_status)
"""

import json
from collections import defaultdict
from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_VALIDATED

def get_db(db=None):
    if db is None:
        client = MongoClient(MONGO_URI)
        return client[MONGO_DB_NAME]
    return db

# تعبير تحويل المبالغ بأمان تام حتى لو احتوى السجل على نصوص أو أرقام عربية (onError=0.0)
SAFE_AMOUNT_CONVERT = {
    "$convert": {
        "input": "$total_amount",
        "to": "double",
        "onError": 0.0,
        "onNull": 0.0
    }
}

# ==============================================================================
# 1. تقرير المبيعات حسب المدينة (Sales by City)
# ==============================================================================
def agg_sales_by_city(db=None, limit=20):
    """
    تحليل حجم المبيعات وعدد الطلبات ومتوسط قيمة السلة لكل مدينة
    """
    db = get_db(db)
    pipeline = [
        {"$match": {"city": {"$ne": None, "$ne": ""}}},
        {
            "$group": {
                "_id": "$city",
                "total_sales": {"$sum": SAFE_AMOUNT_CONVERT},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": SAFE_AMOUNT_CONVERT},
                "min_order_value": {"$min": SAFE_AMOUNT_CONVERT},
                "max_order_value": {"$max": SAFE_AMOUNT_CONVERT}
            }
        },
        {"$sort": {"total_sales": -1}},
        {"$limit": limit}
    ]
    raw_res = list(db[COLLECTION_VALIDATED].aggregate(pipeline))
    return [
        {
            "city": r.get("_id"),
            "total_sales": round(float(r.get("total_sales", 0.0) or 0.0), 2),
            "order_count": int(r.get("order_count", 0)),
            "avg_order_value": round(float(r.get("avg_order_value", 0.0) or 0.0), 2),
            "min_order_value": round(float(r.get("min_order_value", 0.0) or 0.0), 2),
            "max_order_value": round(float(r.get("max_order_value", 0.0) or 0.0), 2)
        }
        for r in raw_res
    ]

# ==============================================================================
# 2. تقرير أفضل المنتجات مبيعاً (Top Products)
# ==============================================================================
def agg_top_products(db=None, limit=20, sample_size=10000):
    """
    تحليل أداء المنتجات من حيث الكميات المباعة وإجمالي الإيرادات المتحققة
    يقرأ من items_json ويجمع الإحصائيات بكفاءة عالية
    """
    db = get_db(db)
    col = db[COLLECTION_VALIDATED]
    prod_stats = defaultdict(lambda: {"name": "", "qty": 0, "revenue": 0.0, "orders_count": 0})
    
    cursor = col.find({"items_json": {"$ne": None}}, {"items_json": 1}).limit(sample_size)
    for doc in cursor:
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
                    
                    prod_stats[sku]["name"] = name
                    prod_stats[sku]["qty"] += qty
                    prod_stats[sku]["revenue"] += rev
                    prod_stats[sku]["orders_count"] += 1
        except Exception:
            continue
            
    results = []
    for sku, data in prod_stats.items():
        results.append({
            "sku": sku,
            "product_name": data["name"],
            "total_quantity": data["qty"],
            "total_revenue": round(data["revenue"], 2),
            "orders_count": data["orders_count"],
            "avg_unit_price": round(data["revenue"] / data["qty"], 2) if data["qty"] > 0 else 0.0
        })
        
    results.sort(key=lambda x: x["total_revenue"], reverse=True)
    return results[:limit]

# ==============================================================================
# 3. تقرير أفضل العملاء إنفاقاً (Top Customers)
# ==============================================================================
def agg_top_customers(db=None, limit=20):
    """
    تحديد كبار العملاء (VIP Customers) حسب إجمالي القيمة الشرائية وعدد الطلبات
    """
    db = get_db(db)
    pipeline = [
        {"$match": {"customer_id": {"$ne": None, "$ne": ""}}},
        {
            "$group": {
                "_id": "$customer_id",
                "customer_name": {"$first": "$customer_name"},
                "city": {"$first": "$city"},
                "total_spent": {"$sum": SAFE_AMOUNT_CONVERT},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": SAFE_AMOUNT_CONVERT},
                "last_order_date": {"$max": "$order_date"}
            }
        },
        {"$sort": {"total_spent": -1}},
        {"$limit": limit}
    ]
    raw_res = list(db[COLLECTION_VALIDATED].aggregate(pipeline))
    return [
        {
            "customer_id": r.get("_id"),
            "customer_name": r.get("customer_name"),
            "city": r.get("city"),
            "total_spent": round(float(r.get("total_spent", 0.0) or 0.0), 2),
            "order_count": int(r.get("order_count", 0)),
            "avg_order_value": round(float(r.get("avg_order_value", 0.0) or 0.0), 2),
            "last_order_date": r.get("last_order_date")
        }
        for r in raw_res
    ]

# ==============================================================================
# 4. تقرير المبيعات عبر الفترات الزمنية (Sales by Period)
# ==============================================================================
def agg_sales_by_period(db=None, period_format="%Y-%m", limit=24):
    """
    تحليل الاتجاه الزمني للإيرادات شهرياً أو يومياً
    """
    db = get_db(db)
    substr_len = 7 if period_format == "%Y-%m" else 10
    pipeline = [
        {"$match": {"order_date": {"$ne": None, "$ne": ""}}},
        {
            "$group": {
                "_id": {"$substr": ["$order_date", 0, substr_len]},
                "total_revenue": {"$sum": SAFE_AMOUNT_CONVERT},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": SAFE_AMOUNT_CONVERT}
            }
        },
        {"$sort": {"_id": 1}},
        {"$limit": limit}
    ]
    raw_res = list(db[COLLECTION_VALIDATED].aggregate(pipeline))
    return [
        {
            "period": r.get("_id"),
            "total_revenue": round(float(r.get("total_revenue", 0.0) or 0.0), 2),
            "total_sales": round(float(r.get("total_revenue", 0.0) or 0.0), 2),
            "order_count": int(r.get("order_count", 0)),
            "avg_order_value": round(float(r.get("avg_order_value", 0.0) or 0.0), 2)
        }
        for r in raw_res
    ]

# ==============================================================================
# 5. تقرير توزيع الطلبات حسب الحالة (Orders by Status)
# ==============================================================================
def agg_orders_by_status(db=None):
    """
    تحليل توزيع الطلبات ونسب الإنجاز والإلغاء مع إجمالي القيمة لكل حالة
    """
    db = get_db(db)
    pipeline = [
        {"$match": {"status": {"$ne": None, "$ne": ""}}},
        {
            "$group": {
                "_id": "$status",
                "order_count": {"$sum": 1},
                "total_revenue": {"$sum": SAFE_AMOUNT_CONVERT},
                "avg_amount": {"$avg": SAFE_AMOUNT_CONVERT}
            }
        },
        {"$sort": {"order_count": -1}}
    ]
    raw_res = list(db[COLLECTION_VALIDATED].aggregate(pipeline))
    total_orders = sum(r.get("order_count", 0) for r in raw_res) or 1
    return [
        {
            "status": r.get("_id"),
            "order_status": r.get("_id"),
            "order_count": int(r.get("order_count", 0)),
            "percentage": round((r.get("order_count", 0) / total_orders) * 100, 2),
            "total_revenue": round(float(r.get("total_revenue", 0.0) or 0.0), 2),
            "total_value": round(float(r.get("total_revenue", 0.0) or 0.0), 2),
            "avg_amount": round(float(r.get("avg_amount", 0.0) or 0.0), 2)
        }
        for r in raw_res
    ]

# قاموس التسجيل الرسمي لكافة التجميعات لخدمة الـ API
REGISTERED_AGGREGATIONS = {
    "sales_by_city": {
        "title": "المبيعات حسب المدينة",
        "description": "تقرير إجمالي المبيعات وعدد الطلبات ومتوسط القيمة لكل مدينة",
        "func": agg_sales_by_city,
        "default_params": {"limit": 10}
    },
    "top_products": {
        "title": "أفضل المنتجات مبيعاً",
        "description": "تقرير المنتجات الأكثر مبيعاً وإيراداً مع تفاصيل الكميات",
        "func": agg_top_products,
        "default_params": {"limit": 10}
    },
    "top_customers": {
        "title": "أفضل العملاء إنفاقاً",
        "description": "تقرير كبار العملاء حسب إجمالي المشتريات ومعدل الطلب",
        "func": agg_top_customers,
        "default_params": {"limit": 10}
    },
    "sales_by_period": {
        "title": "المبيعات عبر الفترات الزمنية",
        "description": "تحليل مسار المبيعات عبر الشهور والأيام",
        "func": agg_sales_by_period,
        "default_params": {"limit": 12}
    },
    "orders_by_status": {
        "title": "توزيع الطلبات حسب الحالة",
        "description": "تحليل نسب وحالات الطلبات وإجمالي مبيعات كل حالة",
        "func": agg_orders_by_status,
        "default_params": {}
    }
}

# أسماء بديلة ملائمة للاستدعاء والاستيراد المباشر
aggregate_sales_by_city = agg_sales_by_city
aggregate_top_products = agg_top_products
aggregate_top_customers = agg_top_customers
aggregate_sales_by_period = agg_sales_by_period
aggregate_orders_by_status = agg_orders_by_status
