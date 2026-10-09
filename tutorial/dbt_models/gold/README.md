# Gold dbt models — ABC Retail

Gold là consumption layer: dữ liệu Silver đã sạch được tổ chức thành star schema
và business marts để BI truy vấn trực tiếp. Bộ này không đọc Landing/Bronze và
không parse lại JSON.

## 1. Những object sẽ được tạo

Tất cả được materialize thành table trong `ABC_RETAIL.GOLD`.

| Model | Grain | Vai trò |
|---|---|---|
| `DIM_DATE` | Một ngày | Calendar dimension |
| `DIM_CUSTOMER` | Một version khách hàng | SCD Type 2, gồm CDC delete/resurrection |
| `DIM_PRODUCT` | Một SKU hiện tại | SCD Type 1, denormalized category |
| `DIM_GEOGRAPHY` | Một country code | Conformed dimension |
| `FACT_CUSTOMER_EVENT` | Một `event_id` | Clickstream transaction fact |
| `FACT_ORDER_LINE` | Một `order_id + line_number` | Order-line transaction fact |
| `FACT_ORDER_STATUS` | Một status/delete event | Accumulated status timeline |
| `MART_DAILY_FUNNEL` | Ngày + device OS | View/cart/checkout/purchase funnel |
| `MART_REVENUE_DAILY` | Ngày + country + category | Gross/net revenue |
| `MART_CUSTOMER_360` | Một current customer | Customer profile và metrics |
| `MART_DATA_QUALITY_DAILY` | Ngày + source + status + error | DQ dashboard |

Surrogate key dùng `SHA2` từ natural key và mốc hiệu lực. Vì vậy key ổn định
qua các lần `dbt build --full-refresh`; không dùng sequence nên không làm orphan
fact sau rebuild.

## 2. Cấu trúc trong Snowflake Workspace

Copy các file local như sau:

```text
abc_retail_dbt/
├── models/
│   ├── silver/
│   │   └── ...
│   └── gold/
│       ├── schema.yml
│       ├── dim_date.sql
│       ├── dim_customer.sql
│       ├── dim_product.sql
│       ├── dim_geography.sql
│       ├── fact_customer_event.sql
│       ├── fact_order_line.sql
│       ├── fact_order_status.sql
│       ├── mart_daily_funnel.sql
│       ├── mart_revenue_daily.sql
│       ├── mart_customer_360.sql
│       └── mart_data_quality_daily.sql
└── tests/
    ├── silver/
    │   └── ...
    └── gold/
        ├── assert_dim_customer_one_current.sql
        ├── assert_dim_customer_scd2_no_overlap.sql
        ├── assert_fact_order_line_grain.sql
        ├── assert_fact_order_line_reconciles_silver.sql
        ├── assert_fact_event_reconciles_silver.sql
        ├── assert_mart_revenue_reconciles_fact.sql
        ├── assert_mart_customer_360_grain.sql
        └── assert_gold_metrics_non_negative.sql
```

## 3. Cập nhật `dbt_project.yml`

Giữ nguyên cấu hình Silver và thêm block `gold` ngang cấp với `silver`:

```yaml
name: abc_retail_dbt
version: "1.0.0"
config-version: 2

profile: abc_retail_dbt

model-paths:
  - models

test-paths:
  - tests

macro-paths:
  - macros

clean-targets:
  - target
  - dbt_packages

vars:
  silver_lookback_days: 7

models:
  abc_retail_dbt:
    silver:
      +schema: SILVER
      +tags:
        - silver
    gold:
      +schema: GOLD
      +tags:
        - gold
```

Không thêm `+materialized` ở folder level. Mỗi model tự khai báo materialization
và `schema='GOLD'` trong `config()`. Block `gold` trong `dbt_project.yml` vẫn
được giữ để tổ chức tag/config ở cấp folder; model-level schema là lớp bảo vệ
để Gold không rơi về `target.schema` khi folder config bị đặt sai tên.

`profiles.yml` không cần đổi. Target `dev` đặt `schema: SILVER` làm fallback cho
các Silver model; Gold model khai báo custom schema `GOLD`, và macro
`generate_schema_name` giữ nguyên custom schema đó.

## 4. Tạo schema và quyền một lần

Trong SQL Worksheet, chạy toàn bộ:

```text
tutorial/sql/06_gold_setup.sql
```

Nếu không có `ACCOUNTADMIN`, nhờ admin tạo `ABC_RETAIL.GOLD` và grant quyền như
file trên. Không cần xóa hay tạo lại các bảng Silver.

## 5. Thứ tự chạy trong Workspace

### Lần đầu

1. Chạy Silver trước và bảo đảm pass: operation **Build**, flags
   `--select tag:silver`.
2. Chọn operation **Compile**, flags `--select tag:gold`.
3. Chọn operation **Build**, flags `--select tag:gold --full-refresh`.

`Build` tự chạy model theo DAG do `ref()` tạo ra, sau đó chạy generic và
singular tests liên quan. Không chạy từng dimension/fact bằng tay.

### Các lần sau

Chọn operation **Build**, flags:

```text
--select tag:gold
```

Với data demo, Gold dùng table rebuild để ưu tiên tính đúng, idempotent và xử
lý CDC delete/re-key hoàn chỉnh. Khi dữ liệu production lớn, có thể chuyển các
fact sang incremental sau khi đã định nghĩa watermark và hard-delete strategy.

## 6. Xác nhận sau build

```sql
SHOW TABLES IN SCHEMA ABC_RETAIL.GOLD;

SELECT TABLE_NAME, ROW_COUNT
FROM ABC_RETAIL.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'GOLD'
ORDER BY TABLE_NAME;

SELECT *
FROM ABC_RETAIL.GOLD.MART_REVENUE_DAILY
ORDER BY REPORT_DATE, COUNTRY_CODE, CATEGORY_CODE
LIMIT 100;

SELECT *
FROM ABC_RETAIL.GOLD.MART_DAILY_FUNNEL
ORDER BY REPORT_DATE, DEVICE_OS
LIMIT 100;

SELECT *
FROM ABC_RETAIL.GOLD.MART_CUSTOMER_360
ORDER BY LIFETIME_REVENUE_USD DESC
LIMIT 100;

SELECT *
FROM ABC_RETAIL.GOLD.MART_DATA_QUALITY_DAILY
ORDER BY METRIC_DATE, SOURCE_NAME, RECORD_STATUS, ERROR_CODE;
```

Trong DQ mart, `CURATED/VALID` là số record đầu ra Silver. `QUALITY_ISSUE` là
số issue; hai nhóm có thể overlap, ví dụ feedback chứa PII vẫn được giữ sau khi
redact nhưng đồng thời có issue `PII_DETECTED`.

## 7. Business rules cần nhớ

- Anonymous funnel dùng `ANALYTIC_USER_ID = 'anonymous:' || SESSION_ID`.
- Net revenue của order `cancelled` hoặc `returned` bằng 0. Đây là rule demo;
  partial refund cần một refund fact riêng.
- Dòng thiếu FX vẫn nằm trong fact với amount USD null và
  `IS_FX_RATE_MISSING = TRUE`; mart đưa amount đó về 0 và công khai số dòng
  thiếu FX, không âm thầm coi tỷ giá là 1.
- Customer fact lookup dimension theo point-in-time:
  `event/order timestamp >= effective_from AND < effective_to`.
- Customer 360 lấy đúng một current SCD2 version, kể cả customer đã soft-delete;
  cột `IS_DELETED` cho phép BI lọc theo policy.
