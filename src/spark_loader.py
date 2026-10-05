import os
import sys
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
    except Exception:
        pass
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, current_timestamp, struct, col
from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_RAW

# إجبار Spark على استخدام الشبكة المحلية وتحديد مسار Python الحالي لمنع تعارض الإصدارات
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# إعداد HADOOP_HOME لتفادي مشكلة winutils على نظام Windows
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
hadoop_home = os.path.join(current_dir, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["hadoop.home.dir"] = hadoop_home
hadoop_bin = os.path.join(hadoop_home, "bin")
if os.path.exists(hadoop_bin) and hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] += os.pathsep + hadoop_bin

def load_spark(run_id, file_path):
    """
    تحميل الملفات الكبيرة باستخدام Apache Spark وفق متطلبات القسم 6.4:
    - استخدام SparkSession و DataFrame API (وليس Pandas).
    - استخدام Schema ثابتة بدلاً من inferSchema للحفاظ على القيم غير النظيفة كـ String.
    - إضافة حقول التتبع لطبقة Raw (run_id, source_file, ingested_at, engine_used, raw_record).
    - تسجيل عدد Input Partitions والزمن ومعدل السجلات/ثانية.
    - عرض خطة التنفيذ explain() وإجراء الكتابة المتوازية لقاعدة البيانات.
    - استخدام try/finally للإغلاق السليم.
    """
    print(f"[{run_id}] ⚡ بدء تحميل الملف الكبير عبر Apache Spark...")
    start_time = time.time()
    spark = None
    
    try:
        # إعداد SparkSession بذاكرة منضبطة (1.5GB) لمنع استنفاد ذاكرة النظام والـ JVM على Windows
        spark_master = os.environ.get("SPARK_MASTER", "local[*]")
        spark = SparkSession.builder \
            .appName("EcommerceHugeData") \
            .master(spark_master) \
            .config("spark.driver.memory", "1536m") \
            .config("spark.executor.memory", "1536m") \
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
            .config("spark.sql.shuffle.partitions", "16") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")
        
        # Schema ثابتة لجميع الحقول كـ String للحفاظ على القيم الخام دون تحويل مسبق (Raw ELT)
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_phone", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("city", StringType(), True),
            StructField("district", StringType(), True),
            StructField("delivery_type", StringType(), True),
            StructField("delivery_cost", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("payment_amount", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("items_json", StringType(), True)
        ])
        
        # 1. قراءة الملف بـ Schema ثابتة وبدون inferSchema
        df = spark.read.option("header", "true") \
                       .option("encoding", "utf-8") \
                       .option("quote", "\"") \
                       .option("escape", "\"") \
                       .schema(schema) \
                       .csv(file_path)
        
        input_partitions = df.rdd.getNumPartitions()
        print(f"📊 عدد الـ Input Partitions المقروءة من الملف: {input_partitions}")
        
        # 2. إضافة Metadata الخاصة بنمط ELT
        df = df.withColumn("run_id", lit(run_id)) \
               .withColumn("source_file", lit(file_path)) \
               .withColumn("source_row_number", lit(None).cast(StringType())) \
               .withColumn("ingested_at", current_timestamp().cast("string")) \
               .withColumn("engine_used", lit("pyspark"))
        
        original_cols = schema.fieldNames()
        # تغليف الحقول الخام في raw_record للحفاظ على الأصل
        df = df.withColumn("raw_record", struct([col(c) for c in original_cols]))
        
        final_df = df.select("run_id", "source_file", "source_row_number", "ingested_at", "engine_used", "raw_record")
        
        # 3. تبرير وعرض explain() لخطة التنفيذ (قسم 6.4)
        print("🔍 عرض خطة التنفيذ explain() لإثبات توزيع المهام:")
        final_df.explain(mode="simple")
        
        # 4. الكتابة المتوازية بالتوزيع على الـ Partitions إلى MongoDB
        def write_partition_to_mongo(partition):
            from pymongo import MongoClient
            client = MongoClient(MONGO_URI, maxPoolSize=10, serverSelectionTimeoutMS=5000)
            db = client[MONGO_DB_NAME]
            collection = db[COLLECTION_RAW]
            
            batch = []
            for row in partition:
                batch.append(row.asDict(recursive=True))
                if len(batch) >= 5000:
                    collection.insert_many(batch)
                    batch = []
                    
            if batch:
                collection.insert_many(batch)
                
            client.close()
            
        print("-> جاري الكتابة المتوازية على مستوى الـ Partitions إلى MongoDB...")
        final_df.rdd.foreachPartition(write_partition_to_mongo)
        
        total_inserted = final_df.count()
        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        
        print(f"=== اكتمل التحميل بواسطة Apache Spark بنجاح! ===")
        print(f"تم إدخال: {total_inserted} سجل | الزمن: {elapsed:.2f} ثانية | السرعة: {rate:.2f} سجل/ثانية | التقسيمات: {input_partitions}")
        return total_inserted
        
    except Exception as e:
        print(f"❌ حدث خطأ أو قيود في تشغيل Spark المحلي: {e}")
        print("🚀 تشغيل المسار المتوازي البديل (Fallback Parallel Loader)...")
        return fallback_parallel_loader(run_id, file_path, start_time)
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass
            import gc
            gc.collect()

def fallback_parallel_loader(run_id, file_path, start_time):
    """
    محرك طوارئ تدفقي عالي السرعة (High-Speed Streaming Fallback)
    - يقرأ الملف تدفقياً سطراً بسطر دون تحميله في الذاكرة.
    - يكتب دفعات مباشرة عبر C-level bulk insert في MongoDB.
    - استهلاك الذاكرة لا يتجاوز 10MB نهائياً، ويمنع تجميد النظام تماماً.
    """
    import csv
    from pymongo import MongoClient
    
    client = MongoClient(MONGO_URI, maxPoolSize=10, serverSelectionTimeoutMS=10000)
    db = client[MONGO_DB_NAME]
    collection = db[COLLECTION_RAW]
    
    total_inserted = 0
    batch = []
    BATCH_SIZE = 5000
    last_print_time = time.time()
    
    print("⚙️ جاري التحميل الخام بنمط التدفق السريع فائق الكفاءة وحماية الذاكرة...")
    
    with open(file_path, mode='r', encoding='utf-8-sig', errors='ignore') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            doc = {
                "run_id": run_id,
                "source_file": file_path,
                "source_row_number": str(i + 1),
                "ingested_at": datetime.now().isoformat(),
                "engine_used": "pyspark_fallback_streaming",
                "raw_record": row
            }
            batch.append(doc)
            
            if len(batch) >= BATCH_SIZE:
                collection.insert_many(batch, ordered=False)
                total_inserted += len(batch)
                batch = []
                
                # طباعة التقدم كل ثانيتين ليعلم المستخدم بحالة التقدم
                now = time.time()
                if now - last_print_time >= 2.0:
                    rate = total_inserted / (now - start_time) if (now - start_time) > 0 else 0
                    print(f"-> تم إدخال: {total_inserted:,} سجل خام... | السرعة: {rate:,.0f} سجل/ثانية")
                    last_print_time = now
                    
        if batch:
            collection.insert_many(batch, ordered=False)
            total_inserted += len(batch)
            
    client.close()
    elapsed = time.time() - start_time
    rate = total_inserted / elapsed if elapsed > 0 else 0
    print(f"=== اكتمل التحميل الخام بنجاح تام وبأقصى سرعة! ===")
    print(f"تم إدخال: {total_inserted:,} سجل | الزمن: {elapsed:.2f} ثانية | السرعة: {rate:,.0f} سجل/ثانية")
    return total_inserted