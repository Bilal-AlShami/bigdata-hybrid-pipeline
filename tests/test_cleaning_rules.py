"""
اختبارات شاملة لقواعد التنظيف الثمانية (Quality Rules)
========================================================
يتم التحقق من:
1. تحويل الأرقام العربية إلى لاتينية
2. تحويل الكلمات العربية إلى أرقام (مثل "ألفان" → 2000)
3. توحيد العملة إلى YER وإزالة النص النقدي
4. إزالة فواصل الآلاف من المبالغ
5. تنظيف رقم الهاتف
6. تصحيح البريد الإلكتروني
7. توحيد صيغة التاريخ إلى YYYY-MM-DD
8. التريم وتوحيد حالة الطلب + إعادة حساب الإجمالي

بالإضافة إلى التحقق من:
- توليد أثر التصحيح (Audit Trail) بشكل صحيح
- حالات العزل (Quarantine) للسجلات التالفة
- السجلات السليمة التي لا تحتاج تعديل
"""

import pytest
import json
import sys
import os

# إضافة مسار المشروع الجذري لتمكين الاستيراد
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.quality_rules import (
    apply_quality_rules,
    clean_arabic_numerals,
    clean_thousands_separator,
    words_to_numbers,
    clean_phone,
    clean_email,
    clean_date,
)


# =====================================================================
# بيانات وهمية (Mock Data) للاختبارات
# =====================================================================

def _make_record(**overrides):
    """ينشئ سجل وهمي كامل مع إمكانية تعديل أي حقل."""
    base = {
        "order_id": "ORD-001",
        "order_date": "2024-01-15",
        "status": "Confirmed",
        "customer_id": "CUST-100",
        "customer_name": "أحمد علي",
        "customer_phone": "777123456",
        "customer_email": "ahmed@example.com",
        "payment_amount": "5000",
        "total_amount": "5500",
        "delivery_cost": "500",
        "currency": "YER",
        "items_json": json.dumps([{"name": "منتج أ", "qty": 1, "price": 5000, "total": 5000}]),
    }
    base.update(overrides)
    return base


# =====================================================================
# القاعدة 1: تحويل الأرقام العربية إلى لاتينية
# =====================================================================

class TestRule1_ArabicNumerals:
    """اختبار قاعدة ARABIC_TO_LATIN_NUMERALS"""

    def test_arabic_digits_converted(self):
        """أرقام عربية في order_id يجب أن تُحوَّل إلى لاتينية"""
        record = _make_record(order_id="ORD-٠٠١", customer_id="CUST-١٠٠")
        cleaned, status, corrections, errors = apply_quality_rules(record)

        assert cleaned["order_id"] == "ORD-001"
        assert cleaned["customer_id"] == "CUST-100"
        assert status == "corrected"

        # التحقق من أثر التصحيح (Audit Trail)
        arabic_corrs = [c for c in corrections if c["rule_code"] == "ARABIC_TO_LATIN_NUMERALS"]
        assert len(arabic_corrs) >= 2

        order_corr = next(c for c in arabic_corrs if c["field"] == "order_id")
        assert order_corr["original_value"] == "ORD-٠٠١"
        assert order_corr["corrected_value"] == "ORD-001"

    def test_latin_digits_unchanged(self):
        """أرقام لاتينية موجودة مسبقاً لا يجب تعديلها"""
        record = _make_record(order_id="ORD-001")
        cleaned, status, corrections, errors = apply_quality_rules(record)

        arabic_corrs = [c for c in corrections if c["rule_code"] == "ARABIC_TO_LATIN_NUMERALS"]
        assert len(arabic_corrs) == 0

    def test_mixed_arabic_latin(self):
        """نص يحتوي على خليط من الأرقام العربية واللاتينية"""
        record = _make_record(customer_phone="٧77١٢٣456")
        cleaned, status, corrections, errors = apply_quality_rules(record)

        assert cleaned["customer_phone"] == "777123456"


# =====================================================================
# القاعدة 2: تحويل الكلمات العربية إلى أرقام
# =====================================================================

class TestRule2_WordsToNumbers:
    """اختبار قاعدة WORDS_TO_NUMBERS"""

    def test_words_converted_in_payment(self):
        """كلمة 'ألفان' في payment_amount يجب أن تتحول إلى 2000"""
        record = _make_record(
            payment_amount="ألفان",
            total_amount="2500",
            delivery_cost="500",
            items_json=json.dumps([{"name": "منتج", "qty": 1, "price": 2000, "total": 2000}]),
        )
        cleaned, status, corrections, errors = apply_quality_rules(record)

        word_corrs = [c for c in corrections if c["rule_code"] == "WORDS_TO_NUMBERS"]
        assert len(word_corrs) >= 1
        assert word_corrs[0]["field"] == "payment_amount"
        assert word_corrs[0]["original_value"] == "ألفان"
        assert word_corrs[0]["corrected_value"] == "2000"

    def test_numeric_value_no_word_conversion(self):
        """قيمة رقمية عادية لا يجب تطبيق قاعدة الكلمات عليها"""
        record = _make_record(payment_amount="3000")
        _, _, corrections, _ = apply_quality_rules(record)

        word_corrs = [c for c in corrections if c["rule_code"] == "WORDS_TO_NUMBERS"]
        assert len(word_corrs) == 0


# =====================================================================
# القاعدة 3: توحيد العملة إلى YER
# =====================================================================

class TestRule3_CurrencyStandardization:
    """اختبار قواعد REMOVE_CURRENCY_TEXT و CURRENCY_STANDARDIZATION"""

    def test_currency_text_removed_from_amount(self):
        """نص 'ريال يمني' يجب إزالته من المبلغ وتوحيد العملة"""
        record = _make_record(
            payment_amount="5000 ريال يمني",
            currency="",
            total_amount="5500",
            delivery_cost="500",
            items_json=json.dumps([{"name": "منتج", "qty": 1, "price": 5000, "total": 5000}]),
        )
        cleaned, status, corrections, errors = apply_quality_rules(record)

        # التحقق من إزالة النص النقدي
        curr_text_corrs = [c for c in corrections if c["rule_code"] == "REMOVE_CURRENCY_TEXT"]
        assert len(curr_text_corrs) >= 1
        assert "5000" in curr_text_corrs[0]["corrected_value"]

        # التحقق من توحيد العملة
        std_corrs = [c for c in corrections if c["rule_code"] == "CURRENCY_STANDARDIZATION"]
        assert len(std_corrs) >= 1
        assert std_corrs[0]["corrected_value"] == "YER"
        assert cleaned["currency"] == "YER"

    def test_yer_currency_no_change(self):
        """عملة YER موجودة مسبقاً ولا يوجد نص نقدي → لا تعديل"""
        record = _make_record(currency="YER", payment_amount="5000")
        _, _, corrections, _ = apply_quality_rules(record)

        curr_corrs = [c for c in corrections if c["rule_code"] in ("REMOVE_CURRENCY_TEXT", "CURRENCY_STANDARDIZATION")]
        assert len(curr_corrs) == 0


# =====================================================================
# القاعدة 4: إزالة فواصل الآلاف
# =====================================================================

class TestRule4_ThousandsSeparator:
    """اختبار قاعدة REMOVE_THOUSANDS_SEPARATOR"""

    def test_comma_removed_from_amount(self):
        """فاصلة الآلاف في total_amount يجب إزالتها"""
        record = _make_record(
            total_amount="5,500",
            payment_amount="5000",
            delivery_cost="500",
            items_json=json.dumps([{"name": "منتج", "qty": 1, "price": 5000, "total": 5000}]),
        )
        cleaned, status, corrections, errors = apply_quality_rules(record)

        assert cleaned["total_amount"] == "5500" or cleaned["total_amount"] == 5500.0
        sep_corrs = [c for c in corrections if c["rule_code"] == "REMOVE_THOUSANDS_SEPARATOR"]
        assert len(sep_corrs) >= 1
        assert sep_corrs[0]["original_value"] == "5,500"
        assert sep_corrs[0]["corrected_value"] == "5500"

    def test_no_comma_no_change(self):
        """مبلغ بدون فاصلة لا يجب تعديله"""
        record = _make_record(total_amount="5500")
        _, _, corrections, _ = apply_quality_rules(record)

        sep_corrs = [c for c in corrections if c["rule_code"] == "REMOVE_THOUSANDS_SEPARATOR"]
        assert len(sep_corrs) == 0


# =====================================================================
# القاعدة 5: تنظيف رقم الهاتف
# =====================================================================

class TestRule5_PhoneFormat:
    """اختبار قاعدة PHONE_FORMAT_STANDARDIZATION"""

    def test_phone_spaces_removed(self):
        """مسافات في رقم الهاتف يجب إزالتها"""
        record = _make_record(customer_phone="777 123 456")
        cleaned, status, corrections, errors = apply_quality_rules(record)

        assert cleaned["customer_phone"] == "777123456"
        phone_corrs = [c for c in corrections if c["rule_code"] == "PHONE_FORMAT_STANDARDIZATION"]
        assert len(phone_corrs) == 1
        assert phone_corrs[0]["original_value"] == "777 123 456"
        assert phone_corrs[0]["corrected_value"] == "777123456"

    def test_phone_plus_removed(self):
        """علامة + في رقم الهاتف يجب إزالتها"""
        record = _make_record(customer_phone="+967777123456")
        cleaned, _, corrections, _ = apply_quality_rules(record)

        assert cleaned["customer_phone"] == "967777123456"
        phone_corrs = [c for c in corrections if c["rule_code"] == "PHONE_FORMAT_STANDARDIZATION"]
        assert len(phone_corrs) == 1

    def test_clean_phone_unchanged(self):
        """رقم هاتف نظيف لا يجب تعديله"""
        record = _make_record(customer_phone="777123456")
        _, _, corrections, _ = apply_quality_rules(record)

        phone_corrs = [c for c in corrections if c["rule_code"] == "PHONE_FORMAT_STANDARDIZATION"]
        assert len(phone_corrs) == 0


# =====================================================================
# القاعدة 6: تصحيح البريد الإلكتروني
# =====================================================================

class TestRule6_EmailFix:
    """اختبار قاعدة EMAIL_REPEATED_SYMBOLS"""

    def test_double_at_fixed(self):
        """علامة @@ مكررة يجب تصحيحها إلى @"""
        record = _make_record(customer_email="ahmed@@example.com")
        cleaned, _, corrections, _ = apply_quality_rules(record)

        assert cleaned["customer_email"] == "ahmed@example.com"
        email_corrs = [c for c in corrections if c["rule_code"] == "EMAIL_REPEATED_SYMBOLS"]
        assert len(email_corrs) == 1
        assert email_corrs[0]["original_value"] == "ahmed@@example.com"
        assert email_corrs[0]["corrected_value"] == "ahmed@example.com"

    def test_double_dot_fixed(self):
        """نقطتان متتاليتان .. يجب تصحيحها إلى نقطة واحدة"""
        record = _make_record(customer_email="ahmed@example..com")
        cleaned, _, corrections, _ = apply_quality_rules(record)

        assert cleaned["customer_email"] == "ahmed@example.com"

    def test_valid_email_unchanged(self):
        """بريد سليم لا يجب تعديله"""
        record = _make_record(customer_email="test@domain.com")
        _, _, corrections, _ = apply_quality_rules(record)

        email_corrs = [c for c in corrections if c["rule_code"] == "EMAIL_REPEATED_SYMBOLS"]
        assert len(email_corrs) == 0


# =====================================================================
# القاعدة 7: توحيد صيغة التاريخ
# =====================================================================

class TestRule7_DateStandardization:
    """اختبار قاعدة DATE_STANDARDIZATION"""

    def test_slash_date_converted(self):
        """تاريخ بصيغة YYYY/MM/DD يجب تحويله إلى YYYY-MM-DD"""
        record = _make_record(order_date="2024/01/15")
        cleaned, status, corrections, errors = apply_quality_rules(record)

        assert cleaned["order_date"] == "2024-01-15"
        date_corrs = [c for c in corrections if c["rule_code"] == "DATE_STANDARDIZATION"]
        assert len(date_corrs) == 1
        assert date_corrs[0]["original_value"] == "2024/01/15"
        assert date_corrs[0]["corrected_value"] == "2024-01-15"

    def test_datetime_format_converted(self):
        """تاريخ بصيغة ISO مع وقت يجب تحويله إلى تاريخ فقط"""
        record = _make_record(order_date="2024-01-15T10:30:00")
        cleaned, _, corrections, _ = apply_quality_rules(record)

        assert cleaned["order_date"] == "2024-01-15"
        date_corrs = [c for c in corrections if c["rule_code"] == "DATE_STANDARDIZATION"]
        assert len(date_corrs) == 1

    def test_dd_mm_yyyy_converted(self):
        """تاريخ بصيغة DD/MM/YYYY يجب تحويله إلى YYYY-MM-DD"""
        record = _make_record(order_date="15/01/2024")
        cleaned, _, corrections, _ = apply_quality_rules(record)

        assert cleaned["order_date"] == "2024-01-15"

    def test_standard_date_unchanged(self):
        """تاريخ بصيغة YYYY-MM-DD سليم لا يجب تعديله"""
        record = _make_record(order_date="2024-01-15")
        _, _, corrections, _ = apply_quality_rules(record)

        date_corrs = [c for c in corrections if c["rule_code"] == "DATE_STANDARDIZATION"]
        assert len(date_corrs) == 0


# =====================================================================
# القاعدة 8: التريم والمرادفات وإعادة حساب الإجمالي
# =====================================================================

class TestRule8_TrimSynonymsRecalculate:
    """اختبار قواعد TRIM_AND_SYNONYMS و RECALCULATE_TOTAL"""

    def test_arabic_status_converted(self):
        """حالة 'مؤكد' يجب تحويلها إلى 'Confirmed'"""
        record = _make_record(status="مؤكد")
        cleaned, _, corrections, _ = apply_quality_rules(record)

        assert cleaned["status"] == "Confirmed"
        trim_corrs = [c for c in corrections if c["rule_code"] == "TRIM_AND_SYNONYMS"]
        assert len(trim_corrs) == 1
        assert trim_corrs[0]["original_value"] == "مؤكد"
        assert trim_corrs[0]["corrected_value"] == "Confirmed"

    def test_paid_status_converted(self):
        """حالة 'مدفوع' يجب تحويلها إلى 'Paid'"""
        record = _make_record(status="مدفوع")
        cleaned, _, corrections, _ = apply_quality_rules(record)

        assert cleaned["status"] == "Paid"

    def test_status_trimmed(self):
        """مسافات زائدة حول الحالة يجب إزالتها"""
        record = _make_record(status="  Confirmed  ")
        cleaned, _, corrections, _ = apply_quality_rules(record)

        assert cleaned["status"] == "Confirmed"
        trim_corrs = [c for c in corrections if c["rule_code"] == "TRIM_AND_SYNONYMS"]
        assert len(trim_corrs) == 1

    def test_total_recalculated(self):
        """إذا كان الإجمالي خاطئاً يجب إعادة حسابه من items + delivery_cost"""
        record = _make_record(
            total_amount="9999",  # قيمة خاطئة عمداً
            delivery_cost="500",
            items_json=json.dumps([
                {"name": "منتج أ", "qty": 2, "price": 1000, "total": 2000},
                {"name": "منتج ب", "qty": 1, "price": 3000, "total": 3000},
            ]),
        )
        cleaned, status, corrections, errors = apply_quality_rules(record)

        # الإجمالي المتوقع = 2000 + 3000 + 500 = 5500
        recalc_corrs = [c for c in corrections if c["rule_code"] == "RECALCULATE_TOTAL"]
        assert len(recalc_corrs) == 1
        assert recalc_corrs[0]["corrected_value"] == 5500.0
        assert cleaned["total_amount"] == 5500.0

    def test_correct_total_unchanged(self):
        """إجمالي صحيح لا يجب إعادة حسابه"""
        record = _make_record(
            total_amount="5500",
            delivery_cost="500",
            items_json=json.dumps([{"name": "منتج", "qty": 1, "price": 5000, "total": 5000}]),
        )
        _, _, corrections, _ = apply_quality_rules(record)

        recalc_corrs = [c for c in corrections if c["rule_code"] == "RECALCULATE_TOTAL"]
        assert len(recalc_corrs) == 0


# =====================================================================
# حالات العزل (Quarantine)
# =====================================================================

class TestQuarantine:
    """اختبار حالات العزل التي تمنع التصحيح"""

    def test_missing_order_id_quarantined(self):
        """سجل بدون order_id يجب عزله"""
        record = _make_record(order_id="")
        _, status, _, errors = apply_quality_rules(record)

        assert status == "quarantined"
        assert "MISSING_ORDER_ID" in errors

    def test_missing_customer_id_quarantined(self):
        """سجل بدون customer_id يجب عزله"""
        record = _make_record(customer_id="")
        _, status, _, errors = apply_quality_rules(record)

        assert status == "quarantined"
        assert "MISSING_CUSTOMER_ID" in errors

    def test_corrupted_json_quarantined(self):
        """items_json تالف يجب أن يعزل السجل"""
        record = _make_record(items_json="NOT_VALID_JSON{{{")
        _, status, _, errors = apply_quality_rules(record)

        assert status == "quarantined"
        assert "CORRUPTED_ITEMS_JSON" in errors

    def test_negative_total_quarantined(self):
        """إجمالي سالب (بعد إعادة الحساب) يجب أن يعزل السجل"""
        record = _make_record(
            total_amount="-500",
            delivery_cost="0",
            items_json=json.dumps([{"name": "منتج", "qty": 1, "price": -600, "total": -600}]),
        )
        _, status, _, errors = apply_quality_rules(record)

        assert status == "quarantined"
        assert "AMBIGUOUS_NEGATIVE_VALUE" in errors

    def test_empty_items_quarantined(self):
        """قائمة items فارغة يجب أن تعزل السجل"""
        record = _make_record(items_json=json.dumps([]))
        _, status, _, errors = apply_quality_rules(record)

        assert status == "quarantined"
        assert "EMPTY_ITEMS" in errors


# =====================================================================
# السجل السليم (Valid Record - لا يحتاج تعديل)
# =====================================================================

class TestValidRecord:
    """اختبار السجلات السليمة التي لا تحتاج أي تعديل"""

    def test_clean_record_is_valid(self):
        """سجل نظيف بالكامل يجب أن يكون حالته 'valid'"""
        record = _make_record()
        cleaned, status, corrections, errors = apply_quality_rules(record)

        assert status == "valid"
        assert len(corrections) == 0
        assert len(errors) == 0

    def test_valid_record_data_preserved(self):
        """البيانات السليمة يجب أن تبقى كما هي بدون تعديل"""
        record = _make_record()
        cleaned, _, _, _ = apply_quality_rules(record)

        assert cleaned["order_id"] == "ORD-001"
        assert cleaned["customer_id"] == "CUST-100"
        assert cleaned["customer_phone"] == "777123456"
        assert cleaned["customer_email"] == "ahmed@example.com"
        assert cleaned["order_date"] == "2024-01-15"
        assert cleaned["status"] == "Confirmed"


# =====================================================================
# أثر التصحيح (Audit Trail) - اختبارات شاملة
# =====================================================================

class TestAuditTrail:
    """اختبار بنية وصحة أثر التصحيح"""

    def test_correction_structure(self):
        """كل تصحيح يجب أن يحتوي على field, original_value, corrected_value, rule_code"""
        record = _make_record(
            order_date="2024/01/15",
            customer_phone="+967 777 123456",
        )
        _, _, corrections, _ = apply_quality_rules(record)

        assert len(corrections) > 0
        for corr in corrections:
            assert "field" in corr, "حقل field مفقود من أثر التصحيح"
            assert "original_value" in corr, "حقل original_value مفقود"
            assert "corrected_value" in corr, "حقل corrected_value مفقود"
            assert "rule_code" in corr, "حقل rule_code مفقود"

    def test_multiple_corrections_tracked(self):
        """سجل يحتاج تصحيحات متعددة يجب أن يتتبعها جميعاً"""
        record = _make_record(
            order_id="ORD-٠٠١",          # قاعدة 1: أرقام عربية
            order_date="2024/01/15",      # قاعدة 7: تاريخ
            customer_phone="777 123 456", # قاعدة 5: هاتف
            status="مؤكد",               # قاعدة 8: مرادفات
        )
        _, status, corrections, _ = apply_quality_rules(record)

        assert status == "corrected"
        rule_codes = [c["rule_code"] for c in corrections]
        assert "ARABIC_TO_LATIN_NUMERALS" in rule_codes
        assert "DATE_STANDARDIZATION" in rule_codes
        assert "PHONE_FORMAT_STANDARDIZATION" in rule_codes
        assert "TRIM_AND_SYNONYMS" in rule_codes

    def test_no_corrections_for_valid_record(self):
        """سجل سليم يجب ألا يحتوي على أي أثر تصحيح"""
        record = _make_record()
        _, status, corrections, errors = apply_quality_rules(record)

        assert status == "valid"
        assert corrections == []
        assert errors == []


# =====================================================================
# اختبارات الدوال المساعدة بشكل مستقل (Unit Tests)
# =====================================================================

class TestHelperFunctions:
    """اختبار الدوال المساعدة بشكل منفصل"""

    def test_clean_arabic_numerals_basic(self):
        assert clean_arabic_numerals("٠١٢٣٤٥٦٧٨٩") == "0123456789"

    def test_clean_arabic_numerals_mixed(self):
        assert clean_arabic_numerals("سعر: ٥٠٠٠ ريال") == "سعر: 5000 ريال"

    def test_clean_arabic_numerals_non_string(self):
        assert clean_arabic_numerals(12345) == 12345

    def test_clean_thousands_separator_with_comma(self):
        val, changed = clean_thousands_separator("1,000")
        assert val == "1000"
        assert changed is True

    def test_clean_thousands_separator_no_comma(self):
        val, changed = clean_thousands_separator("1000")
        assert val == "1000"
        assert changed is False

    def test_words_to_numbers_alfan(self):
        val, changed = words_to_numbers("ألفان")
        assert val == "2000"
        assert changed is True

    def test_words_to_numbers_no_match(self):
        val, changed = words_to_numbers("5000")
        assert val == "5000"
        assert changed is False

    def test_clean_phone_spaces(self):
        val, changed = clean_phone("777 123 456")
        assert val == "777123456"
        assert changed is True

    def test_clean_email_double_at(self):
        val, changed = clean_email("a@@b.com")
        assert val == "a@b.com"
        assert changed is True

    def test_clean_date_slash(self):
        val, changed = clean_date("2024/01/15")
        assert val == "2024-01-15"
        assert changed is True

    def test_clean_date_standard(self):
        val, changed = clean_date("2024-01-15")
        assert val == "2024-01-15"
        assert changed is False
