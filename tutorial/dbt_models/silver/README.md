# Silver dbt models — ABC Retail

Bộ model này thay thế toàn bộ Silver cũ. Thiết kế mới xử lý:

- Typed parsing và semantic validation.
- Deduplicate có thứ tự xác định.
- Late/stale update không ghi đè current state mới hơn.
- Order và customer current state theo SCD Type 1.
- Lịch sử customer changes để tạo SCD Type 2 ở Core DWH.
- Order status history.
- CDC delete theo đúng `business_key`, `changed_at`, `operation`, `source_lsn`.
- Soft delete và khả năng resurrect nếu upsert mới hơn delete.
- Order item FX conversion sang USD.
- Unknown SKU duoc loai khoi order items hop le va ghi vao semantic quarantine.
- PII detection và email-redacted feedback dành cho Cortex AI.
- Semantic rejection model và macro đồng bộ vào quarantine.

## 1. Cấu trúc cần tạo trong Snowflake Workspace

```text
abc_retail_dbt/
├── models/
│   └── staging/
│       ├── sources.yml
│       ├── stg_events.sql
│       ├── stg_order_changes.sql
│       ├── stg_orders_current.sql
│       ├── stg_order_status_history.sql
│       ├── stg_order_items.sql
│       ├── stg_customer_changes.sql
│       ├── stg_customers_current.sql
│       ├── stg_products.sql
│       ├── stg_fx_rates.sql
│       ├── stg_customer_feedback.sql
│       ├── stg_cdc_deletes.sql
│       └── dq_silver_rejections.sql
├── macros/
│   └── governance/
│       └── sync_silver_rejections.sql
└── tests/
    └── silver/
        ├── assert_unique_order_item_grain.sql
        ├── assert_unique_fx_rate_grain.sql
        ├── assert_orders_current_not_stale.sql
        ├── assert_customers_current_not_stale.sql
        ├── assert_cdc_delete_events_not_lost.sql
        └── assert_soft_delete_state.sql
```

Các file SQL/YAML trong thư mục local này được copy vào `models/staging/`.
Các singular test nằm ở `tutorial/dbt_tests/silver/` và phải copy vào
`tests/silver/` của Workspace.

## 2. Cấu hình dbt project

Đảm bảo `dbt_project.yml` có:

```yaml
model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

vars:
  silver_lookback_days: 7

models:
  abc_retail_dbt:
    staging:
      +schema: SILVER
      +tags: ["silver"]
```

Model-level `config()` quyết định `table` hay `incremental`, nên không cần đặt
`+materialized` cho cả folder staging.

`dq_silver_rejections.sql` đặt `schema='GOVERNANCE'`; role `ABC_ENGINEER` phải
có `USAGE` và `CREATE TABLE` trên `ABC_RETAIL.GOVERNANCE`.

## 3. Lần chạy chuyển đổi từ Silver cũ

Các model cũ không có những column mới như `state_updated_ingest_ts`,
`is_deleted`, `change_id`. Vì vậy lần đầu phải chạy full refresh.

Trong Workspace chọn operation **Build** và Additional flags:

```text
--select tag:silver --full-refresh
```

Không dùng **Run Operation** cho câu lệnh trên.

Sau khi build pass, chọn **Run Operation** và nhập macro:

```text
sync_silver_rejections
```

Các lần chạy bình thường sau đó dùng **Build**:

```text
--select tag:silver
```

## 4. Xác nhận nhanh

```sql
select count(*) from ABC_RETAIL.SILVER.STG_CDC_DELETES;
-- Data hiện tại: 300

select count(*) from ABC_RETAIL.SILVER.STG_PRODUCTS;
-- Data hiện tại: 1000

select count(*) from ABC_RETAIL.SILVER.STG_FX_RATES;
-- Data hiện tại: 64

select count(*) from ABC_RETAIL.SILVER.STG_CUSTOMER_FEEDBACK;
-- Data hiện tại: 4800

select status, count(*)
from ABC_RETAIL.SILVER.STG_ORDERS_CURRENT
group by status
order by status;
-- Phải nhìn thấy returned nếu source có returned.

select entity_type, count(*)
from ABC_RETAIL.SILVER.STG_CDC_DELETES
group by entity_type;
-- order = 200, customer = 100.

select error_code, count(*)
from ABC_RETAIL.GOVERNANCE.SILVER_REJECTIONS
group by error_code
order by error_code;
```

Không đặt điều kiện `line_amount_usd is not null` cho toàn bộ order items:
GBP được cố ý thiếu trong FX source để tạo lỗi `FX_RATE_MISSING`.
