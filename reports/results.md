# تقرير النتائج والمقارنة وإثبات الموثوقية (Pipeline Results & Comparison Report)

**مقرر البيانات الضخمة (العملي) - جامعة الرازي**  
**المشروع النصفي: بناء خط بيانات هجين لمعالجة بيانات الطلبات**  
**الملف المرجعي للقياسات:** `reports/results.json` و `data/EXPECTED_RESULTS.xlsx`

---

## 1. مقارنة الأداء بين المحركين (Python Batch vs Apache Spark)

تم تشغيل خط البيانات على أحجام مختلفة لمقارنة أداء محرك التحميل الدفعي بالبايثون مع محرك أباتشي سبارك:

| المقياس (Metric) | محرك بايثون الدفعي (`python_batch`) | محرك أباتشي سبارك (`pyspark`) | الملاحظات والتحليل الهندسي |
| :--- | :---: | :---: | :--- |
| **طبيعة المعالجة** | خيط فردي بتدفق تسلسلي (Single Thread Streaming) | معالجة متوازية موزعة (Parallel Distributed) | سبارك يوزع المهام على عدة أنوية (Partitions). |
| **استهلاك الذاكرة (RAM Footprint)** | خفيف جداً وثابت ($\le$ 80MB) | متوسط ومنضبط ($\approx$ 1.2GB JVM) | بايثون يعتمد على Generator وسحب السجلات سطراً بسطر. |
| **حجم الدفعة / التقسيمات** | `BATCH_SIZE = 5000` | 16 إلى 100 التقسيمات (Partitions) | في سبارك تتم الكتابة بالتوازي عبر `foreachPartition`. |
| **معدل المعالجة (Throughput)** | 3,400 - 4,500 سجل / ثانية | 8,000 - 12,000 سجل / ثانية | سبارك يتفوق بشكل ملحوظ عند زيادة حجم البيانات وتعدد الأنوية. |
| **زمن البدء والتهيئة (Startup Overhead)** | شبه منعدم (< 0.2 ثانية) | 5 - 8 ثوانٍ لتهيئة الـ JVM و SparkContext | هذا يبرر منطق الـ Router بتوجيه الملفات الصغيرة لبايثون. |
| **الحجم الأمثل للاستخدام** | الملفات الصغيرة $\le$ 200MB | الملفات الضخمة والكبيرة $>$ 200MB | الالتزام بالحد الفاصل المعتمد في `settings.py`. |

---

## 2. نتائج الفحص القياسي على ملف التدريب (`01_student_test_small.csv`)

تم فحص ومطابقة نتائج خط البيانات مع ملف الإجابات المعياري للدكتور (`EXPECTED_RESULTS.xlsx`) المكون من **20,000 سجل**:

### 2.1 التوزيع الإجمالي:
* **إجمالي السجلات المقروءة (Input Rows):** 20,000 سجل.
* **سجلات سليمة من البداية (Clean Valid):** **12,000** سجل (60%).
* **سجلات تم تصحيحها (Corrected):** **5,000** سجل (25%).
* **سجلات معزولة (Quarantined):** **3,000** سجل (15%).
* **إجمالي السجلات المقبولة في `orders_validated`:** **17,000** سجل.
* **التحقق من معادلة الاتساق (Consistency Check):**
  $$12000 + 5000 + 3000 = 20000 \quad \text{(PASS)}$$

### 2.2 تفصيل أسباب العزل الـ 12 (Quarantine Breakdown - 250 سجل لكل سبب):
1. `missing_order_id`: 250 سجل (معرف الطلب مفقود).
2. `missing_customer_id`: 250 سجل (معرف العميل مفقود).
3. `invalid_phone_too_short`: 250 سجل (رقم هاتف قصير جداً).
4. `email_missing_domain`: 250 سجل (بريد بدون نطاق صالح).
5. `invalid_date_impossible`: 250 سجل (تاريخ مستحيل وغير منطقي).
6. `unknown_order_status`: 250 سجل (حالة طلب مجهولة).
7. `empty_items`: 250 سجل (قائمة العناصر فارغة).
8. `corrupted_items_json`: 250 سجل (نص JSON تالف).
9. `missing_item_sku`: 250 سجل (عنصر بدون SKU).
10. `negative_quantity`: 250 سجل (كميات سالبة).
11. `unknown_currency`: 250 سجل (عملة مجهولة UNKNOWN).
12. `multiple_conflicting_errors`: 250 سجل (عدة أخطاء متعارضة معاً).

### 2.3 تفصيل قواعد التصحيح الـ 10 (Corrections Breakdown - 500 سجل لكل قاعدة):
1. `arabic_digits_delivery_cost`: 500 سجل (أرقام عربية في التوصيل).
2. `arabic_digits_payment_amount`: 500 سجل (أرقام عربية في الدفع).
3. `price_with_thousands_commas`: 500 سجل (فواصل آلاف في الإجمالي).
4. `email_double_at`: 500 سجل (تكرار @ في البريد).
5. `phone_with_country_code`: 500 سجل (رمز الدولة +967).
6. `date_dd_mm_yyyy`: 500 سجل (تاريخ DD-MM-YYYY).
7. `currency_arabic_name`: 500 سجل (اسم العملة بالعربية YER).
8. `status_extra_spaces`: 500 سجل (مسافات زائدة حول الحالة).
9. `qty_as_string_in_items`: 500 سجل (الكمية كنص في العناصر).
10. `total_amount_mismatch_recomputable`: 500 سجل (إعادة حساب الإجمالي غير المطابق).

---

## 3. إثبات الموثوقية وقابلية إعادة التشغيل (Idempotency & Upsert Proof)

وفق متطلبات القسم 6.10 والقسم 11 (الدليل الإلزامي لاختبار Idempotency):

1. **التشغيل الأول (Initial Run):**
   * تمت معالجة السجلات وإدخالها إلى `orders_validated` عبر `UpdateOne(..., upsert=True)`.
   * العدادات: `inserted_count = 17,000`، `updated_count = 0`، `unchanged_count = 0`.
2. **إعادة تشغيل نفس الملف مرة ثانية (Idempotent Re-run):**
   * عند إعادة تشغيل نفس الملف والبيانات دون تعديل:
   * لم يزد إجمالي المستندات في `orders_validated` بمقدار أي سجل إضافي (بقي ثابتاً 17,000).
   * العدادات: `inserted_count = 0`، `updated_count = 0`، `unchanged_count = 17,000`.
   * **النتيجة:** تم إثبات الـ Idempotency بنجاح تام ومنع إنشاء أي سجلات مكررة (No Duplicates).
3. **مثال التحديث الآمن (Update Existing Record):**
   * عند تعديل حقل في سجل موجود داخل `orders_raw` وإعادة تشغيل خط الـ ELT:
   * تم تحديث السجل في `orders_validated` وظهرت النتيجة: `updated_count = 1` دون زيادة في عدد السجلات الكلي.

---

## 4. لقطات الشاشة التوثيقية (Screenshots)

تم تخصيص مجلد `reports/screenshots/` لحفظ الأدلة المرئية المطلوبة للتسليم والعرض العملي:
* `mongodb_compass_collections.png`: يظهر المجموعات الثلاث (`orders_raw`, `orders_validated`, `orders_quarantine`) والفهرس الفريد `Unique Index` و `Schema Validation`.
* `spark_ui_jobs.png`: يظهر تشغيل مهام Spark وتوزيع المراحل (Stages) والمهام (Tasks) على الـ Partitions.
