# ĐỀ BÀI THỰC HÀNH SNOWFLAKE END-TO-END

## 1. Bối cảnh

Công ty **ABC Retail** vận hành một nền tảng thương mại điện tử đa kênh. Dữ liệu phát sinh từ website, ứng dụng di động, hệ thống quản lý đơn hàng và CRM được chuyển vào vùng landing dưới dạng file theo giờ.

Bạn được giao xây dựng một nền tảng phân tích trên **Snowflake** để:

- Nạp và xử lý dữ liệu theo kiến trúc Medallion: **Bronze → Silver → Core DWH → Gold/Data Mart**.
- Phục vụ dashboard bán hàng, customer 360, funnel và data quality.
- Hỗ trợ incremental processing, CDC, dữ liệu đến trễ, retry và schema drift.
- Lưu lịch sử thay đổi của khách hàng bằng SCD.
- Minh họa dimensional modeling, star schema, snowflake schema, normalization và denormalization.
- Thực hành các tính năng Snowflake: Stages, `COPY INTO`, Tables, Streams, Tasks, Stored Procedures, Dynamic Tables, Zero-Copy Cloning, Time Travel, Fail-safe, Secure Data Sharing và Cortex AI.

## 2. Mục tiêu học tập

Sau khi hoàn thành project, người thực hiện phải giải thích và chứng minh được:

1. Vai trò của từng layer trong Medallion Architecture.
2. Sự khác nhau giữa database, data warehouse, data mart và lakehouse.
3. SCD Type 1 và SCD Type 2; cách xử lý late-arriving dimension.
4. Sự khác nhau giữa source CDC, Snowflake Stream và xử lý theo watermark.
5. Cách thiết kế fact, dimension, grain, natural key, surrogate key và conformed dimension.
6. Trade-off giữa normalization và denormalization; star schema và snowflake schema.
7. Cách load/unload dữ liệu an toàn, idempotent và có audit.
8. Cách tổ chức pipeline bằng Streams, Tasks, Task Graph và Stored Procedures.
9. Cách sử dụng Time Travel, Zero-Copy Cloning, Fail-safe và Secure Data Sharing đúng mục đích.
10. Cách sử dụng Cortex AI trên một use case có thể đánh giá được chất lượng và chi phí.

## 3. Kiến trúc mục tiêu

```text
Source files
    |
    v
Internal/External Stage
    |
    | COPY INTO hoặc Snowpipe
    v
BRONZE / RAW
    |
    | Streams + Tasks + SQL/Stored Procedures
    v
SILVER / CONFORMED
    |
    | MERGE, SCD, data quality, business rules
    v
CORE_DWH
    |-- Dimensions
    |-- Facts
    |
    | Dynamic Tables hoặc Streams + Tasks
    v
GOLD / DATA_MART
    |
    |-- BI dashboards
    |-- Secure Data Sharing
    |-- Cortex AI
    v
Data consumers
```

## 4. Phạm vi kỹ thuật và quy ước

### 4.1. Yêu cầu bắt buộc

- Thực hiện bằng Snowflake SQL; Stored Procedure có thể dùng Snowflake Scripting, SQL hoặc Python.
- Mỗi lần chạy pipeline phải có `batch_id` hoặc `run_id`.
- Pipeline phải idempotent: chạy lại cùng input không tạo dữ liệu nghiệp vụ bị trùng.
- Mọi record Bronze phải truy vết được source file và thời điểm ingest.
- Record không hợp lệ phải được đưa vào quarantine, không được silently discard.
- Incremental pipeline không được phụ thuộc hoàn toàn vào full refresh.
- Tất cả timestamp nghiệp vụ phải được chuẩn hóa về UTC.
- Các đối tượng phải được phân chia rõ theo database/schema và có naming convention nhất quán.

### 4.2. Cấu trúc gợi ý

Có thể dùng một database và nhiều schema:

```text
ABC_RETAIL
├── LANDING
├── BRONZE
├── SILVER
├── CORE_DWH
├── MART
├── GOVERNANCE
└── SANDBOX
```

Hoặc tách thành nhiều database nếu muốn thực hành cloning/sharing ở cấp database.

Virtual warehouse tối thiểu:

- `INGEST_WH`: load/unload dữ liệu.
- `TRANSFORM_WH`: transformation và Tasks.
- `BI_WH`: truy vấn data mart.

Thiết lập auto-suspend và auto-resume phù hợp để tránh lãng phí credit.

## 5. Dữ liệu đầu vào

Mỗi file phải có nhiều batch, bao gồm cả dữ liệu hợp lệ và dữ liệu cố ý gây lỗi.

### 5.1. `clickstream_events` — JSON

Các trường:

- `event_id`: string, khóa tự nhiên của sự kiện.
- `event_time`: ISO8601 string, có thể khác timezone.
- `ingest_time`: ISO8601 string, có thể null.
- `user_id`: string, có thể null.
- `session_id`: string.
- `event_type`: `view`, `add_to_cart`, `checkout`, `purchase` hoặc giá trị không hợp lệ.
- `page`: string.
- `device`: object `{os, model}` hoặc string dạng `ios|iphone`.
- `attributes`: object tùy biến.

Tình huống kiểm thử:

- Trùng `event_id` do retry.
- Event đến trễ tối đa 7 ngày.
- `device` thay đổi kiểu dữ liệu.
- Timestamp sai format.
- Event không có `user_id`; phải nhận diện bằng `session_id`.

### 5.2. `orders` — JSON hoặc Parquet

Các trường:

- `order_id`: string.
- `order_time`: timestamp/string.
- `user_id`: string.
- `items`: array gồm `sku`, `qty`, `unit_price`.
- `currency`: string.
- `discount_code`: string, có thể null.
- `shipping_address`: object.
- `status`: `created`, `paid`, `shipped`, `cancelled`, `returned`.
- `updated_at`: timestamp.

Tình huống kiểm thử:

- Một order xuất hiện nhiều lần với `updated_at` và status khác nhau.
- Update cũ đến sau update mới.
- Thiếu SKU, quantity âm hoặc price âm.
- Currency không được hỗ trợ hoặc thiếu tỷ giá.
- File được upload lại với cùng tên và nội dung.

### 5.3. `customers` — CSV hoặc JSON

Các trường:

- `user_id`: string.
- `email`: string, có thể null hoặc sai định dạng.
- `phone`: string, có thể null.
- `country_code`: string.
- `created_at`: timestamp.
- `updated_at`: timestamp.

Tình huống kiểm thử:

- Email, phone hoặc country thay đổi nhiều lần.
- Hai phiên bản có cùng `updated_at`.
- Một thay đổi cũ đến trễ sau phiên bản current.
- Record không có cả email và phone.

### 5.4. `products` — CSV

Các trường:

- `sku`: string.
- `product_name`: string.
- `brand`: string.
- `subcategory_code`: string.
- `subcategory_name`: string.
- `category_code`: string.
- `category_name`: string.
- `unit_cost`: number.
- `updated_at`: timestamp.

Dữ liệu này được dùng để thực hành product dimension, normalization và snowflake schema.

### 5.5. `fx_rates` — CSV

Các trường:

- `rate_date`: date.
- `source_currency`: string.
- `target_currency`: mặc định `USD`.
- `rate`: number.

Phải có ít nhất một ngày bị thiếu tỷ giá để kiểm thử data quality.

### 5.6. `customer_feedback` — JSON hoặc CSV

Các trường:

- `feedback_id`: string.
- `user_id`: string.
- `order_id`: string, có thể null.
- `feedback_text`: string.
- `created_at`: timestamp.

Dữ liệu này được dùng cho phần Cortex AI.

## 6. Phần A — Thiết lập Snowflake và Load/Unload

### A1. Object setup

1. Tạo role, database, schema và virtual warehouses cần thiết.
2. Áp dụng nguyên tắc least privilege ở mức cơ bản.
3. Tạo:
   - Named internal stage.
   - File formats cho CSV, JSON và Parquet.
   - Permanent table cho dữ liệu chính.
   - Transient table cho staging/intermediate data.
   - Temporary table trong ít nhất một bước xử lý.
4. Giải thích khi nào dùng permanent, transient và temporary table.

### A2. Load

1. Upload file vào internal stage bằng SnowSQL hoặc Snowflake CLI.
2. Kiểm tra file bằng `LIST` và truy vấn trực tiếp staged data trước khi load.
3. Load vào Bronze bằng `COPY INTO <table>`.
4. Ghi các metadata tối thiểu:
   - `source_file`
   - `source_row_number` nếu khả dụng
   - `file_last_modified`
   - `ingest_ts`
   - `batch_id`
   - `raw_payload`
5. Dùng chế độ validation hoặc load history để chứng minh:
   - File nào load thành công.
   - File nào bị lỗi.
   - Snowflake xử lý thế nào khi load lại cùng file.
6. Thiết kế một phương án continuous ingestion:
   - Ưu tiên Snowpipe nếu môi trường hỗ trợ.
   - Nếu không, dùng scheduled/triggered Task và ghi rõ giới hạn.

### A3. Unload

1. Unload `mart_revenue_daily` ra stage bằng `COPY INTO @stage`.
2. Xuất ít nhất hai định dạng: CSV và Parquet.
3. Tổ chức output theo `report_date` hoặc một prefix hợp lý.
4. Dùng `LIST` để xác nhận output.
5. Giải thích các lựa chọn về file size, compression, overwrite và bảo mật dữ liệu.

## 7. Phần B — Medallion Architecture

### B1. Bronze layer

Tạo các bảng:

- `bronze_clickstream`
- `bronze_orders`
- `bronze_customers`
- `bronze_products`
- `bronze_fx_rates`
- `bronze_customer_feedback`

Yêu cầu:

1. Bronze là append-only.
2. Giữ raw payload bằng `VARIANT` đối với JSON.
3. Không sửa dữ liệu source tại Bronze.
4. Mỗi record phải có metadata lineage.
5. Tạo bảng `quarantine_records` lưu:
   - `source_name`
   - `source_file`
   - `batch_id`
   - `raw_payload`
   - `error_code`
   - `error_message`
   - `quarantined_at`
6. Chứng minh cách xử lý schema drift mà không làm mất dữ liệu cũ.

### B2. Silver layer

Tạo các bảng:

- `silver_events`
- `silver_orders_current`
- `silver_order_status_history`
- `silver_order_items`
- `silver_customers_changes`
- `silver_products`
- `silver_fx_rates`
- `silver_customer_feedback`

Yêu cầu chung:

- Parse và ép kiểu bằng các hàm `TRY_*` phù hợp.
- Chuẩn hóa timestamp về UTC.
- Deduplicate bằng business key và thứ tự ưu tiên xác định rõ.
- Record lỗi đi vào quarantine.
- Mỗi rule phải có mã lỗi ổn định.

#### Silver Events

1. Chuẩn hóa `device` thành các cột `device_os`, `device_model`.
2. Deduplicate theo `event_id`.
3. Xử lý anonymous user bằng một khóa phân tích từ `user_id` hoặc `session_id`.
4. Xử lý late events trong cửa sổ 7 ngày.

#### Silver Orders

1. Duy trì latest state theo `order_id` bằng SCD Type 1.
2. Duy trì riêng status timeline để không mất lịch sử.
3. Flatten `items` thành `silver_order_items`.
4. Quy định grain và khóa của order item.
5. Lưu số tiền nguyên bản và số tiền quy đổi USD.
6. Không ghi đè order mới bằng record đến trễ có `updated_at` cũ hơn.

#### Silver Customers

1. Chuẩn hóa email, phone và country.
2. Phát hiện thay đổi theo `email`, `phone`, `country_code`.
3. Giữ change records phục vụ việc tạo SCD Type 2 ở Core DWH.
4. Định nghĩa rule cho record không có cả email và phone.

## 8. Phần C — CDC, Streams, Tasks và Stored Procedures

### C1. CDC

Phân biệt trong tài liệu thiết kế:

- **Source CDC**: thay đổi được phát ra từ hệ thống nguồn.
- **Snowflake Stream**: offset/change tracking trên một Snowflake object.
- **Watermark incremental**: lọc theo `updated_at` hoặc `ingest_ts`.

Nếu input chỉ là file snapshot, không được gọi đó là source CDC. Hãy mô phỏng CDC bằng nhiều phiên bản của cùng business key và giải thích giới hạn.

### C2. Streams

1. Tạo Stream cho ít nhất:
   - `bronze_orders`
   - `bronze_customers`
   - Một Silver/Core table dùng để cập nhật mart.
2. Sử dụng các metadata columns của Stream để phân biệt insert, update và delete nếu nguồn có các thao tác này.
3. Chứng minh rằng nhiều consumer độc lập cần stream/offset riêng.
4. Ghi lại cách phòng tránh stream bị stale.

### C3. Stored Procedures

Tạo tối thiểu hai procedures:

1. `sp_process_orders(run_id)`:
   - Validate dữ liệu.
   - Ghi quarantine.
   - Upsert latest order.
   - Ghi order status history.
   - Ghi audit.
2. `sp_process_customers_scd2(run_id)`:
   - Xử lý SCD2.
   - Xử lý record đến trễ.
   - Đảm bảo chỉ có một current row trên mỗi customer.
   - Ghi audit và lỗi.

Procedure phải xử lý transaction và exception rõ ràng. Khi thất bại, audit không được báo `SUCCESS`.

### C4. Tasks và Task Graph

Tạo pipeline có dependency:

```text
load_bronze
    |
    +--> process_events
    +--> process_orders
    +--> process_customers
              |
              v
       build_core_dwh
              |
              v
        refresh_marts
              |
              v
         data_quality
```

Yêu cầu:

1. Có ít nhất một scheduled Task.
2. Có ít nhất một triggered Task dựa trên `SYSTEM$STREAM_HAS_DATA`.
3. Có Task gọi Stored Procedure.
4. Có Task Graph gồm các bước chạy song song và tuần tự.
5. Theo dõi kết quả bằng task history.
6. Chứng minh pipeline không ghi trùng khi retry.

### C5. Dynamic Tables

Chọn một mart phù hợp và xây dựng thêm một phiên bản bằng Dynamic Table.

So sánh:

- Streams + Tasks: imperative, kiểm soát DML và side effect.
- Dynamic Tables: declarative, định nghĩa kết quả bằng query và target lag.

Không cần thay toàn bộ pipeline bằng Dynamic Tables. Phải giải thích vì sao chọn cơ chế nào cho từng layer.

## 9. Phần D — SCD và Dimensional Modeling

### D1. Core dimensions

Tạo tối thiểu:

- `dim_date`
- `dim_customer`
- `dim_product`
- `dim_device`
- `dim_geography`

#### `dim_customer` — SCD Type 2

Các cột tối thiểu:

- `customer_key`: surrogate key.
- `user_id`: natural key.
- Các thuộc tính customer.
- `effective_from`.
- `effective_to`.
- `is_current`.
- `record_hash`.
- `created_run_id`.
- `updated_run_id`.

Yêu cầu:

1. Không tạo version mới nếu tracked attributes không đổi.
2. Chỉ có một current row cho mỗi `user_id`.
3. Khoảng hiệu lực không overlap.
4. Late-arriving change phải được chèn đúng vào timeline, không chỉ bỏ qua.
5. Fact phải lookup đúng `customer_key` dựa trên business timestamp.

#### `dim_product`

Triển khai hai phương án để so sánh:

1. **Star variant**: category và subcategory được denormalize vào `dim_product`.
2. **Snowflake variant**:
   - `dim_product`
   - `dim_subcategory`
   - `dim_category`

### D2. Core facts

Tạo tối thiểu:

- `fact_order_line`
- `fact_customer_event`
- `fact_order_status`

Trước khi tạo mỗi fact, phải ghi rõ:

- Business process.
- Grain.
- Dimensions liên quan.
- Measures.
- Loại fact: transaction, periodic snapshot hoặc accumulating snapshot.
- Cách xử lý late-arriving fact/dimension.

Ví dụ grain:

- `fact_order_line`: một dòng cho mỗi `order_id + line_number` hoặc `order_id + sku`, tùy rule đã chọn.
- `fact_customer_event`: một dòng cho mỗi `event_id`.
- `fact_order_status`: một dòng cho mỗi lần order thay đổi trạng thái.

### D3. Normalization vs Denormalization

Thực hiện một báo cáo so sánh:

1. Query doanh thu theo category và country trên star schema.
2. Query tương đương trên snowflake schema.
3. So sánh:
   - Số bảng và số join.
   - Khả năng tái sử dụng dimension.
   - Độ đơn giản đối với BI users.
   - Data consistency.
   - Storage.
   - Query profile và execution time.

Không kết luận chỉ dựa trên một lần chạy vì Snowflake có caching. Phải ghi rõ trạng thái cache và warehouse khi đo.

## 10. Phần E — Gold Layer và Data Marts

Tạo tối thiểu năm marts:

### E1. `mart_daily_funnel`

- Grain: một dòng/ngày/country/device.
- Unique visitors tại các bước `view → add_to_cart → checkout → purchase`.
- Có conversion rate giữa từng bước.
- Có rule rõ ràng cho anonymous users.

### E2. `mart_revenue_daily`

- Grain: một dòng/ngày/country/category/currency reporting.
- Gross revenue, discount, refund, net revenue, order count và units sold.
- Cancelled/returned phải dựa trên status timeline, không chỉ latest status nếu business question cần lịch sử.

### E3. `mart_customer_360`

- Một dòng/current customer.
- Tổng orders, completed orders, returned orders.
- Lifetime revenue.
- First/last purchase.
- Favorite category.
- Most-used device.
- Last activity timestamp.

Đây là ví dụ denormalized mart dành cho consumption, không thay thế dimension/fact trong Core DWH.

### E4. `mart_product_performance`

- Doanh thu, units, buyers, return rate theo product/category.
- So sánh kỳ hiện tại với kỳ trước.

### E5. `mart_data_quality_daily`

- Tổng records, valid records, quarantined records và duplicate records.
- Phân loại theo ngày, source, batch và error code.
- Tính data quality pass rate.

## 11. Phần F — Audit, Data Quality và Observability

### F1. Audit

Tạo bảng `audit_pipeline_runs`:

- `run_id`
- `pipeline_name`
- `task_name`
- `batch_id`
- `started_at`
- `ended_at`
- `status`
- `rows_read`
- `rows_inserted`
- `rows_updated`
- `rows_deleted`
- `rows_quarantined`
- `source_files`
- `query_id`
- `error_message`

### F2. Data quality rules

Tối thiểu phải có:

- `missing_key`
- `invalid_timestamp`
- `schema_mismatch`
- `duplicate_key`
- `invalid_email`
- `invalid_quantity`
- `invalid_price`
- `unsupported_currency`
- `fx_rate_missing`
- `scd_overlap`
- `multiple_current_rows`
- `orphan_fact`

### F3. Reconciliation

Chứng minh bằng SQL:

```text
rows_read = rows_valid + rows_quarantined + rows_deduplicated
```

Nếu công thức thực tế khác do một record có nhiều lỗi hoặc flatten thành nhiều rows, phải đưa ra reconciliation rule tương ứng.

## 12. Phần G — Snowflake Data Protection

### G1. Zero-Copy Cloning

1. Clone Core DWH hoặc toàn bộ database sang môi trường development.
2. Chạy thử một thay đổi logic trên clone.
3. Chứng minh clone ban đầu không copy vật lý toàn bộ dữ liệu.
4. Thay đổi dữ liệu ở clone và chứng minh source không bị thay đổi.
5. Ghi nhận storage/cost implication khi source và clone tiếp tục thay đổi.

### G2. Time Travel

Thực hiện các kịch bản:

1. Query table tại một thời điểm trước đó bằng `AT` hoặc `BEFORE`.
2. Xóa hoặc update nhầm dữ liệu rồi phục hồi.
3. Drop một table và khôi phục bằng `UNDROP`.
4. Clone một historical version để điều tra lỗi mà không ghi đè production.
5. Giải thích retention period và chi phí lưu trữ.

### G3. Fail-safe

Trình bày và chứng minh bằng tài liệu:

- Fail-safe không phải cơ chế truy vấn lịch sử hằng ngày.
- Fail-safe bắt đầu sau khi Time Travel retention kết thúc.
- Việc khôi phục Fail-safe do Snowflake hỗ trợ trong trường hợp đặc biệt, không phải thao tác self-service như Time Travel.
- Transient và temporary tables có đặc tính bảo vệ dữ liệu khác permanent tables.

Không yêu cầu cố tình gây sự cố để gọi Snowflake Support.

## 13. Phần H — Secure Data Sharing

Giả lập chia sẻ `mart_revenue_daily` và `mart_product_performance` cho đối tác.

Yêu cầu:

1. Tạo secure view chỉ chứa dữ liệu được phép chia sẻ.
2. Loại bỏ hoặc mask PII.
3. Tạo share và grant đúng object.
4. Nếu có consumer account, tạo database từ share và kiểm tra read-only access.
5. Nếu không có consumer account, cung cấp đầy đủ DDL và bằng chứng grant.
6. Giải thích vì sao Secure Data Sharing không cần copy/unload dữ liệu.
7. So sánh Secure Data Sharing với việc export file qua stage.

Bonus:

- Row access policy theo partner.
- Masking policy cho email/phone.
- Reader account hoặc listing nếu môi trường hỗ trợ.

## 14. Phần I — Cortex AI

Sử dụng `customer_feedback` để xây dựng một enrichment table:

`mart_feedback_ai_enriched`

Các trường gợi ý:

- `feedback_id`
- `sentiment`
- `feedback_category`
- `summary`
- `model_or_function_used`
- `processed_at`
- `prompt_version`
- `ai_run_id`

Yêu cầu:

1. Dùng Cortex AI Functions phù hợp, ưu tiên bề mặt hàm AI hiện hành như:
   - `AI_SENTIMENT`
   - `AI_CLASSIFY`
   - `AI_COMPLETE`
2. Không gửi email, phone hoặc PII không cần thiết vào prompt.
3. Thiết kế incremental processing để feedback cũ không bị gọi model lại không cần thiết.
4. Tạo tập kiểm thử được gán nhãn thủ công.
5. Đánh giá ít nhất:
   - Classification accuracy.
   - Null/error rate.
   - Latency.
   - Chi phí ước tính.
6. Lưu prompt version và audit để kết quả có thể truy vết.
7. Trình bày cách xử lý output không ổn định hoặc không đúng schema.

Nếu account/region không hỗ trợ Cortex AI, phải:

- Viết DDL và query dự kiến.
- Dùng mock result để hoàn thành downstream mart.
- Ghi rõ phần nào chưa thể chạy và lý do.

## 15. Phần J — Performance và Cost

Không áp dụng trực tiếp các lệnh Delta Lake như `OPTIMIZE`, `ZORDER` hoặc `VACUUM`.

Thay vào đó, thực hiện:

1. Kiểm tra Query Profile của ít nhất ba truy vấn.
2. Đánh giá micro-partition pruning.
3. Thử clustering key trên một bảng đủ lớn hoặc trình bày khi nào chưa nên dùng.
4. So sánh warehouse size cho một workload có kiểm soát.
5. Giải thích:
   - Result cache.
   - Warehouse cache.
   - Auto-suspend/auto-resume.
   - Multi-cluster warehouse.
6. Thiết lập Resource Monitor nếu quyền cho phép.
7. Đưa ra ít nhất ba quyết định tối ưu chi phí có số liệu hoặc lập luận rõ ràng.

Bonus:

- Search Optimization Service.
- Materialized View.
- Automatic Clustering.

Không bật dịch vụ có chi phí bổ sung nếu chưa đánh giá tác động.

## 16. Bộ test bắt buộc

Project phải có test hoặc validation query cho các tình huống:

1. Chạy lại cùng file không tạo duplicate business rows.
2. Cùng `event_id` xuất hiện ở hai file.
3. Order update cũ đến sau update mới.
4. Customer change cũ được chèn đúng vào SCD2 timeline.
5. Không có hai current rows cho cùng customer.
6. SCD2 effective ranges không overlap.
7. Fact lookup đúng customer version tại thời điểm order.
8. Missing FX rate được quarantine hoặc xử lý theo rule đã công bố.
9. Anonymous events được tính nhất quán.
10. Task retry không làm sai audit và row count.
11. Stream được consume đúng một lần cho mỗi consumer.
12. Time Travel khôi phục được dữ liệu bị thay đổi.
13. Clone thay đổi độc lập với source.
14. Secure view không lộ PII.
15. AI enrichment không xử lý lại record không thay đổi.

## 17. Deliverables

Repository cần có cấu trúc tối thiểu:

```text
.
├── README.md
├── architecture/
│   ├── architecture_diagram.*
│   ├── dimensional_model.*
│   └── design_decisions.md
├── data/
│   ├── batch_01/
│   ├── batch_02/
│   └── expected_results/
├── sql/
│   ├── 00_roles_warehouses.sql
│   ├── 01_databases_schemas.sql
│   ├── 02_stages_file_formats.sql
│   ├── 03_bronze_tables.sql
│   ├── 04_load_bronze.sql
│   ├── 05_silver_tables.sql
│   ├── 06_streams.sql
│   ├── 07_procedures.sql
│   ├── 08_tasks.sql
│   ├── 09_dimensions.sql
│   ├── 10_facts.sql
│   ├── 11_marts.sql
│   ├── 12_dynamic_tables.sql
│   ├── 13_data_quality.sql
│   ├── 14_clone_time_travel.sql
│   ├── 15_data_sharing.sql
│   ├── 16_cortex_ai.sql
│   └── 17_unload.sql
├── tests/
│   ├── test_data_quality.sql
│   ├── test_idempotency.sql
│   ├── test_scd2.sql
│   └── test_reconciliation.sql
└── evidence/
    ├── query_profiles/
    ├── task_history/
    └── screenshots/
```

README của bài làm phải bao gồm:

- Hướng dẫn chạy từ đầu.
- Thứ tự thực thi scripts.
- Kiến trúc và data flow.
- Data model và grain.
- Các assumption/business rules.
- Cách chạy test.
- Những tính năng không thể chạy do edition, region hoặc quyền.
- Chi phí và cách cleanup resources.

## 18. Tiêu chí nghiệm thu

### 18.1. Core data engineering — 30%

- Load/unload, lineage và audit: 8%.
- Bronze/Silver, schema drift và quarantine: 8%.
- CDC, Streams, Tasks và Stored Procedures: 10%.
- Idempotency và late-arriving data: 4%.

### 18.2. Data modeling — 25%

- SCD1/SCD2 đúng: 8%.
- Grain, fact/dimension và surrogate key: 8%.
- Star/snowflake schema: 5%.
- Normalization/denormalization analysis: 4%.

### 18.3. Data marts và data quality — 15%

- Business marts đúng logic: 8%.
- Data quality, reconciliation và observability: 7%.

### 18.4. Snowflake platform features — 20%

- Cloning, Time Travel và Fail-safe: 7%.
- Dynamic Tables và performance/cost: 5%.
- Secure Data Sharing: 4%.
- Cortex AI: 4%.

### 18.5. Engineering quality — 10%

- Cấu trúc source code và naming: 3%.
- Test evidence: 4%.
- Documentation và giải thích design decisions: 3%.

## 19. Các mốc triển khai gợi ý

### Milestone 1 — Foundation

- Setup Snowflake objects.
- Sinh dữ liệu.
- Stage, load/unload và Bronze.

### Milestone 2 — Incremental pipeline

- Silver.
- Streams, Tasks, Stored Procedures.
- Audit, quarantine và retry.

### Milestone 3 — Data warehouse

- SCD1/SCD2.
- Dimensions và facts.
- Star/snowflake comparison.

### Milestone 4 — Consumption

- Gold marts.
- Dynamic Table.
- Data Sharing và Cortex AI.

### Milestone 5 — Reliability

- Tests.
- Time Travel và cloning.
- Performance/cost review.
- Documentation và cleanup.

## 20. Điều kiện hoàn thành

Project được coi là hoàn thành khi:

1. Pipeline chạy end-to-end bằng dữ liệu của ít nhất ba batch.
2. Có batch chứa update, duplicate, late data và invalid records.
3. Chạy lại pipeline không làm sai dữ liệu.
4. SCD2 và fact-to-dimension lookup vượt qua bộ test.
5. Data marts đối soát được với Core DWH.
6. Có bằng chứng chạy Tasks, Streams, Time Travel và cloning.
7. Data Sharing và Cortex AI được chạy hoặc có phương án mô phỏng hợp lệ nếu môi trường không hỗ trợ.
8. Có tài liệu giải thích các quyết định thiết kế và trade-off.
9. Tất cả warehouse/task/resource thử nghiệm đã được suspend hoặc cleanup sau khi hoàn thành.

