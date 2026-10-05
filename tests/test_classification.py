"""
اختبارات التصنيف والعزل ومعادلة الاتساق الأساسية (Classification & Consistency Tests)
================================================================================
وفق متطلبات القسم 6.8 و 6.11 من وثيقة التكليف الرسمي:
- تصنيف كل سجل بدقة إلى Valid أو Corrected أو Quarantined.
- التحقق من كافة أسباب ورموز العزل (Quarantine Error Codes).
- التحقق من معادلة الاتساق الأساسية:
  run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count
- التحقق من بنية أثر التصحيح (Audit Trail) للسجلات المصححة.
"""

import pytest
import json
import sys
import os

# إضافة مسار المشروع لتمكين الاستيراد
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.quality_rules import apply_quality_rules

def _make_base_record(**overrides):
    """سجل تجريبي قياسي سليم"""
    record = {
        "order_id": "ORD-1001",
        "order_date": "2026-02-15T14:30:00",
        "status": "Confirmed",
        "customer_id": "CUST-5001",
        "customer_name": "سامي محمد",
        "customer_phone": "771234567",
        "customer_email": "sami@example.com",
        "city": "صنعاء",
        "district": "السبعين",
        "delivery_type": "عادي",
        "delivery_cost": "2000.0",
        "payment_method": "نقدًا",
        "payment_status": "مدفوع",
        "payment_amount": "25000.0",
        "currency": "YER",
        "total_amount": "27000.0",
        "items_json": json.dumps([
            {"sku": "SKU-01", "name": "شاحن", "qty": 1, "unit_price": 25000.0, "total": 25000.0}
        ])
    }
    record.update(overrides)
    return record


class TestClassificationOutcomes:
    """اختبار حالات التصنيف الأساسية: Valid, Corrected, Quarantined"""

    def test_clean_record_classification(self):
        """السجل السليم الخالي من أي أخطاء يصنف كـ valid"""
        record = _make_base_record()
        cleaned, status, corrections, errors = apply_quality_rules(record)
        assert status == "valid", "السجل السليم يجب أن يصنف كـ valid"
        assert len(errors) == 0, "السجل السليم لا يجب أن يحوي أي أخطاء عزل"
        assert len(corrections) == 0, "السجل السليم لا يحتاج أي تصحيحات"

    def test_correctable_record_classification(self):
        """السجل الذي يحوي خطأ قابلاً للتصحيح يصنف كـ corrected مع Audit Trail"""
        record = _make_base_record(customer_email="sami@@example.com")
        cleaned, status, corrections, errors = apply_quality_rules(record)
        assert status == "corrected", "السجل القابل للإصلاح يجب أن يصنف كـ corrected"
        assert len(errors) == 0, "السجل المصحح لا يجب أن يرسل للعزل"
        assert len(corrections) == 1, "يجب تسجيل عملية التصحيح في Audit Trail"
        assert corrections[0]["field"] == "customer_email"
        assert corrections[0]["corrected_value"] == "sami@example.com"

    def test_quarantined_record_classification(self):
        """السجل ذو الخطأ الجسيم غير القابل للتصحيح الآمن ينتهي في quarantine"""
        record = _make_base_record(order_id="")
        cleaned, status, corrections, errors = apply_quality_rules(record)
        assert status == "quarantined", "السجل فاقد المعرف يجب أن يعزل"
        assert any(e in ("missing_order_id", "MISSING_ORDER_ID") for e in errors)


class TestQuarantineReasonsCoverage:
    """اختبار تغطية كافة أسباب العزل المنصوص عليها في وثيقة المشروع وملف النتائج المعيارية"""

    def test_missing_order_id(self):
        record = _make_base_record(order_id="")
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert any(e in ("missing_order_id", "MISSING_ORDER_ID") for e in errors)

    def test_missing_customer_id(self):
        record = _make_base_record(customer_id="")
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert any(e in ("missing_customer_id", "MISSING_CUSTOMER_ID") for e in errors)

    def test_invalid_phone_too_short(self):
        record = _make_base_record(customer_phone="12345")
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert "invalid_phone_too_short" in errors

    def test_email_missing_domain(self):
        record = _make_base_record(customer_email="customer_without_domain")
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert "email_missing_domain" in errors

    def test_invalid_date_impossible(self):
        record = _make_base_record(order_date="2026-19-45 99:70:00")
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert any(e in ("invalid_date_impossible", "INVALID_IMPOSSIBLE_DATE") for e in errors)

    def test_unknown_order_status(self):
        record = _make_base_record(status="حالة غامضة غير معروفة")
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert "unknown_order_status" in errors

    def test_unknown_currency(self):
        record = _make_base_record(currency="UNKNOWN")
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert "unknown_currency" in errors

    def test_corrupted_items_json(self):
        record = _make_base_record(items_json="invalid_json_text{")
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert any(e in ("corrupted_items_json", "CORRUPTED_ITEMS_JSON") for e in errors)

    def test_empty_items(self):
        record = _make_base_record(items_json="[]")
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert any(e in ("empty_items", "EMPTY_ITEMS") for e in errors)

    def test_missing_item_sku(self):
        items = [{"name": "وصلة HDMI", "qty": 1, "unit_price": 5000.0, "total": 5000.0}]
        record = _make_base_record(items_json=json.dumps(items))
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert "missing_item_sku" in errors

    def test_negative_quantity(self):
        items = [{"sku": "SKU-01", "name": "سماعة", "qty": -2, "unit_price": 5000.0, "total": -10000.0}]
        record = _make_base_record(items_json=json.dumps(items))
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert "negative_quantity" in errors

    def test_unknown_price(self):
        record = _make_base_record(total_amount="???")
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert any(e in ("unknown_price", "UNKNOWN_PRICE") for e in errors)

    def test_multiple_conflicting_errors(self):
        record = _make_base_record(
            customer_id="",
            customer_email="@@",
            total_amount="???",
            items_json="not-json"
        )
        _, status, _, errors = apply_quality_rules(record)
        assert status == "quarantined"
        assert any(e in ("multiple_conflicting_errors", "MULTIPLE_CONFLICTING_ERRORS") for e in errors)


class TestConsistencyEquation:
    """
    التحقق من قاعدة الاتساق الأساسية (القسم 6.11):
    run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count
    كل سجل خام يجب أن ينتهي بنتيجة واحدة فقط قطعية.
    """

    def test_batch_consistency(self):
        records = [
            _make_base_record(order_id="ORD-01"),                                     # Valid
            _make_base_record(order_id="ORD-02", customer_email="user@@test.com"),     # Corrected
            _make_base_record(order_id="ORD-03", delivery_cost="٢٠٠٠"),               # Corrected
            _make_base_record(order_id=""),                                            # Quarantined
            _make_base_record(order_id="ORD-05", status="حالة غامضة غير معروفة"),      # Quarantined
            _make_base_record(order_id="ORD-06", items_json="[]"),                     # Quarantined
            _make_base_record(order_id="ORD-07"),                                     # Valid
        ]

        raw_count = len(records)
        valid_count = 0
        corrected_count = 0
        quarantine_count = 0

        for rec in records:
            _, status, _, _ = apply_quality_rules(rec)
            if status == "valid":
                valid_count += 1
            elif status == "corrected":
                corrected_count += 1
            elif status == "quarantined":
                quarantine_count += 1
            else:
                pytest.fail(f"حالة غير متوقعة: {status}")

        assert valid_count == 2
        assert corrected_count == 2
        assert quarantine_count == 3
        # التحقق من المعادلة الأساسية
        assert raw_count == (valid_count + corrected_count + quarantine_count), "فشل معادلة الاتساق الحسابية"
