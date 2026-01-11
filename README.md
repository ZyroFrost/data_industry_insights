# Data Industry Insights
### Phân tích thị trường lao động ngành Data toàn cầu

---

## 🧭 1. MÔ TẢ VẤN ĐỀ NGHIÊN CỨU

### 1.1. Tổng quan
Đề tài tập trung vào việc phân tích và khai phá dữ liệu thị trường lao động ngành **Data** trên phạm vi toàn cầu trong giai đoạn **2020–2025**.  
Thông qua dữ liệu tuyển dụng được thu thập và xử lý từ nhiều nguồn, đồ án nhằm khám phá các mẫu hình (patterns), xu hướng và insight liên quan đến:
- Nhu cầu tuyển dụng
- Kỹ năng yêu cầu
- Mức lương
- Hình thức làm việc
- Sự khác biệt giữa các thị trường quốc gia

### 1.2. Mục tiêu nghiên cứu
- Khám phá dữ liệu (EDA) để hiểu rõ đặc điểm và cấu trúc thị trường việc làm ngành Data  
- Phân tích phân bố lương, kỹ năng, vai trò công việc và hình thức làm việc  
- Phân cụm thị trường lao động theo quốc gia bằng thuật toán **KMeans**  
- Áp dụng **PCA** để giảm chiều dữ liệu và hỗ trợ phân tích trực quan  
- Cung cấp insight phục vụ:
  - Định hướng nghề nghiệp
  - Phân tích thị trường lao động
  - Hỗ trợ ra quyết định trong tuyển dụng và hoạch định nhân sự

### 1.3. Nguồn dữ liệu
Dữ liệu được thu thập từ các nguồn tuyển dụng công khai và API, sau đó được xử lý thông qua pipeline gồm các bước:
- Thu thập dữ liệu  
- Làm sạch và chuẩn hóa  
- Enrichment và mapping  
- Tổng hợp dữ liệu phục vụ phân tích  

Tập dữ liệu ban đầu sau bước thu thập gồm khoảng **1.1 triệu bản ghi**.  
Sau quá trình lọc, chuẩn hóa và loại bỏ các bản ghi không đầy đủ thông tin cần thiết cho phân tích, tập dữ liệu cuối cùng dùng cho EDA và modeling còn khoảng **513,000 bản ghi**.

Do đặc thù của thị trường dữ liệu tuyển dụng, các bộ dữ liệu lịch sử đầy đủ trong giai đoạn xa thường là **paid datasets**. Vì vậy, tập dữ liệu phân tích trong đồ án này **tập trung chủ yếu vào giai đoạn 2023–2025**, là khoảng thời gian có dữ liệu công khai đầy đủ và đáng tin cậy nhất, phản ánh sát thực trạng thị trường lao động ngành Data hiện nay.

---

## 📊 2. MÔ TẢ DATASET VÀ CÁC CỘT DỮ LIỆU

### 2.1. Dataset chính
Dataset được sử dụng cho phân tích EDA, PCA và KMeans là tập dữ liệu đã được xử lý hoàn chỉnh từ pipeline.

- Số dòng: ~513,000  
- Dạng dữ liệu: CSV  
- Mục đích: Phân tích thị trường lao động ngành Data

### 2.2. Chi tiết các cột dữ liệu

| STT | Tên cột | Kiểu dữ liệu | Vai trò | Mô tả |
|----:|--------|-------------|--------|------|
| 1 | skill_name | string | INPUT | Tên kỹ năng |
| 2 | skill_category | string | INPUT | Nhóm kỹ năng |
| 3 | certification_required | boolean | INPUT | Yêu cầu chứng chỉ |
| 4 | company_name | string | INPUT | Tên công ty |
| 5 | company_size | string | INPUT | Quy mô công ty |
| 6 | industry | string | INPUT | Ngành nghề công ty |
| 7 | city | string | INPUT | Thành phố làm việc |
| 8 | country | string | INPUT | Quốc gia |
| 9 | country_iso | string | INPUT | Mã ISO quốc gia |
|10 | latitude | float | Feature | Vĩ độ |
|11 | longitude | float | Feature | Kinh độ |
|12 | population | integer | Feature | Dân số |
|13 | role_name | string | INPUT | Chức danh công việc |
|14 | level | string | INPUT | Cấp độ nghề nghiệp |
|15 | department | string | INPUT | Bộ phận |
|16 | employment_type | string | INPUT | Loại hình làm việc |
|17 | skill_level_required | string | INPUT | Mức độ yêu cầu kỹ năng |
|18 | posted_date | date | Feature | Ngày đăng tuyển |
|19 | min_salary | float | INPUT | Lương tối thiểu |
|20 | max_salary | float | INPUT | Lương tối đa |
|21 | currency | string | INPUT | Đơn vị tiền tệ |
|22 | required_exp_years | float | INPUT | Số năm kinh nghiệm yêu cầu |
|23 | education_level | string | INPUT | Trình độ học vấn |
|24 | job_description | string | Feature | Mô tả công việc |
|25 | remote_option | string | INPUT | Hình thức làm việc (Remote/Onsite/Hybrid) |

### 2.3. Phân loại Input / Output

**Biến Input (Features):**
- skill_name, skill_category, certification_required  
- company_name, company_size, industry  
- city, country, country_iso  
- role_name, level, department  
- employment_type, skill_level_required  
- min_salary, max_salary, currency  
- required_exp_years, education_level  
- remote_option  

**Biến Feature hỗ trợ phân tích:**
- latitude, longitude, population  
- posted_date  
- job_description  

### 2.4. Đặc điểm dữ liệu
- Loại dữ liệu: Chủ yếu là **categorical**, kết hợp với một số **numerical** (lương, kinh nghiệm, dân số)  
- Dữ liệu đã được làm sạch, chuẩn hóa và enrich trong pipeline  
- Dataset đủ lớn và đa dạng để phục vụ EDA, clustering và dimensionality reduction

### 2.5. Các cột được sử dụng cho phân tích và modeling

Trong các bước phân tích EDA, KMeans và PCA, **không phải toàn bộ các cột INPUT đều được đưa trực tiếp vào mô hình**.

Các cột được sử dụng làm **đầu vào phân tích (analysis features)** bao gồm:
- Thông tin thị trường: `country`, `city`
- Thông tin vai trò: `role_name`, `level`, `department`
- Thông tin kỹ năng: `skill_name`, `skill_category`, `skill_level_required`
- Thông tin hình thức làm việc: `employment_type`, `remote_option`
- Thông tin thu nhập: `min_salary`, `max_salary`
- Thông tin quy mô hỗ trợ phân tích: `population`, `posted_date`

Các cột như `job_description`, `latitude`, `longitude` được sử dụng cho **enrichment, mô tả và trực quan hóa**,  
**không được đưa trực tiếp vào các thuật toán KMeans hoặc PCA**.

---

## 🔍 3. TỔNG HỢP KẾT QUẢ 7 BƯỚC EDA CƠ BẢN

Quá trình Phân tích Khám phá Dữ liệu (EDA) được thực hiện trên tập dữ liệu khoảng **513.000 dòng** nhằm mục đích hiểu rõ cấu trúc thị trường lao động ngành dữ liệu toàn cầu.

### 3.1. Thu thập và Tổng quan (Data Overview)
Dữ liệu bao gồm các thông tin về:
- Quốc gia  
- Chức danh  
- Kỹ năng  
- Hình thức làm việc  
- Kinh nghiệm  
- Lương (USD)

Phân tích tập trung vào các biến chính ảnh hưởng đến **thu nhập** và **nhu cầu tuyển dụng**.

### 3.2. Kiểm tra dữ liệu khuyết thiếu (Data Integrity)
- Giai đoạn 2021–2022 có số lượng dữ liệu quan sát được thấp do **giới hạn trong khả năng thu thập dữ liệu tuyển dụng công khai**.  
- Các dữ liệu lịch sử đầy đủ cho giai đoạn này chủ yếu thuộc paid datasets, vì vậy giai đoạn 2021–2022 **không được sử dụng để đánh giá xu hướng thị trường**, mà được xem là **giới hạn của tập dữ liệu**.

### 3.3. Làm sạch và Chuẩn hóa (Data Cleaning)
- Quy đổi tất cả đơn vị tiền tệ về **USD** để đảm bảo tính đồng nhất.  
- Nhóm các chức danh công việc vào **10 nhóm chính**  
  (ví dụ: Data Engineer, Data Analyst, Data Scientist).

### 3.4. Thống kê mô tả (Descriptive Statistics)
- **Lương trung vị (Median):** 104,022.06 USD  
- **Lương trung bình (Average):** 143,098.19 USD  

Kết luận: Dữ liệu có phân phối **lệch phải (right-skewed)** do ảnh hưởng của các mức lương cao tại thị trường Mỹ.

### 3.5. Phân tích đơn biến
- **Thị trường:** Mỹ dẫn đầu với **37.3%** thị phần bài đăng tuyển dụng.  
- **Kỹ năng:** SQL (**23.2%**) và Python (**22.8%**) là hai kỹ năng “phải có”.  
- **Hình thức làm việc:** **87.9%** công việc yêu cầu làm việc tại văn phòng (Onsite).

### 3.6. Phân tích mối tương quan giữa các biến
- **Kinh nghiệm vs Lương:** Tồn tại tương quan thuận mạnh mẽ (kinh nghiệm tăng thì lương tăng).  
- **Thời gian:** Thị trường có xu hướng chuyển dịch từ nhóm đặc thù (2023) sang thị trường đại chúng (2025).

### 3.7. Phát hiện ngoại lệ (Outliers)
- Sử dụng **Boxplot** để xác định các mức lương trên **300,000 USD** là ngoại lệ.  
- Các ngoại lệ chủ yếu rơi vào các vai trò **chuyên gia cao cấp** hoặc **lãnh đạo** tại thị trường Mỹ.

---

## 🎯 4. KẾT QUẢ PHÂN TÍCH GOM CỤM (K-MEANS)

Dựa trên các đặc trưng thị trường như **quy mô**, **kỹ năng** và **hình thức làm việc**, thuật toán **K-Means** được áp dụng để phân nhóm các quốc gia.

### 4.1. Các biến đầu vào cho KMeans

Phân tích gom cụm KMeans được thực hiện ở **cấp độ quốc gia**, với dữ liệu đã được tổng hợp và chuẩn hóa từ dataset gốc.

Các biến đầu vào chính cho KMeans bao gồm:
- Số lượng bài đăng tuyển dụng theo quốc gia
- Số lượng vai trò công việc (role diversity)
- Số lượng kỹ năng yêu cầu (skill diversity)
- Phân bố hình thức làm việc (remote / onsite)
- Thông tin thu nhập tổng hợp (salary statistics)
- Thông tin quy mô hỗ trợ phân tích như dân số (`population`)

Các biến này phản ánh **quy mô**, **mức độ đa dạng kỹ năng** và **đặc điểm làm việc** của từng thị trường,
tạo cơ sở cho việc phân nhóm các quốc gia có đặc điểm thị trường lao động tương đồng.

### 4.2. Xác định số cụm tối ưu (Elbow Method)
- Sử dụng phương pháp Elbow để chọn số cụm K=4. Đây là điểm mà tổng bình phương sai lệch trong cụm giảm ổn định, giúp phân loại thị trường rõ rệt nhất.
<p align="center">
  <img src="https://github.com/user-attachments/assets/d833945e-e676-4936-b370-6a283189b065"
       width="659" height="433" />
</p>
<p align="center"><b>Hình 4.2.A: Elbow Method for Optimal K</b></p>
<br>
  
### 4.3. Trực quan hóa các cụm trên không gian PCA
Do dữ liệu có nhiều chiều, **PCA** được sử dụng để giảm chiều và trực quan hóa trên không gian **2D** và **3D**.

#### 4.3.A. Biểu đồ cụm 2D (PC1 vs PC2)
Giúp quan sát sự phân hóa giữa nhóm thị trường khổng lồ (Mỹ) và các nhóm thị trường truyền thống hoặc linh hoạt.

<p align="center">
  <img src="https://github.com/user-attachments/assets/19eeb8fb-f3bb-4678-92e5-6670e6c77f1a"
       width="577" height="459" />
</p>
<p align="center"><b>Hình 4.2.A: Biểu đồ PCA 2D (PC1 vs PC2) – Phân cụm thị trường lao động toàn cầu</b></p>
<br>

#### 4.3.B. Biểu đồ cụm 3D
Cung cấp cái nhìn sâu hơn về sự phân tách của các cụm khi bổ sung thêm chiều về sự đa dạng vai trò công việc.

<p align="center">
  <img src="https://github.com/user-attachments/assets/6006c307-1884-47e0-a269-d4efcb401d40"
       width="550" height="471" />
</p>
<p align="center"><b>Hình 4.2.B: Biểu đồ PCA 3D – Cấu trúc đa tầng của thị trường lao động toàn cầu</b></p>
<br>

### 4.4. Đặc điểm các cụm
- **Cụm 0 (Thị trường dẫn đầu):** Quy mô cực lớn, đa dạng kỹ năng (USA).  
- **Cụm 1 (Thị trường Onsite):** Tỷ lệ làm việc toàn thời gian và tại văn phòng cao (Châu Âu).  
- **Cụm 2 (Thị trường linh hoạt):** Ưu tiên Remote, quy mô vừa (Startup).  
- **Cụm 3 (Thị trường chuyên biệt):** Ít bài đăng hơn nhưng yêu cầu kỹ năng rất cao (Singapore, Mexico).

---

## 🧠 5. PHÂN TÍCH PCA & K-MEANS TRÊN CÁC THÀNH PHẦN CHÍNH (PC)

### 5.1. PCA và Phân cụm KMeans phân tích thị trường toàn cầu

<p align="center">
  <img src="https://github.com/user-attachments/assets/3a578878-4ad6-43bb-92c4-2b42fe77bdd9"
       width="975" height="715" />
</p>
<p align="center"><b>Hình 5.1.1: PCA phân tích vị thế thị trường lao động toàn cầu</b></p>
<br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/37070fb8-ba65-44b8-a513-47e1b809811b"
       width="975" height="715" />
</p>
<p align="center"><b>Hình 5.1.2: Phân cụm KMeans Phân khúc thị trường toàn cầu</b></p>
<br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/f351d1b0-7d72-4985-bbbb-dbc15958470b"
       width="602" height="145" />
</p>
<p align="center"><b>Hình 5.1.3: Phân tích đặc trưng của các cụm sau khi chạy KMeans</b></p>
<br>

#### Ý nghĩa các thành phần chính của PCA:

- **PC1 (Trục hoành - 47.19%)**: Đại diện cho **Quy mô** (Số lượng job và dân số càng lớn thì càng nằm về bên phải).
- **PC2 (Trục tung - 37.61%)**: Đại diện cho **Mật độ/Độ sôi động** (Mật độ việc làm càng cao thì càng nằm phía trên).
- **K-means (Phân cụm)**: Tự động nhóm các quốc gia có đặc điểm tương đồng thành 4 phân khúc chiến lược (Khổng lồ, Sôi động, Đang phát triển, Nhỏ) thay vì chỉ nhìn vào từng quốc gia riêng lẻ.
  
#### Ý nghĩa kết quả:

- **Nhóm "Outliers đặc biệt" (Như Kiribati)**: Nằm tách biệt hẳn ở phía trên trục PC2, cho thấy đây là thị trường có mật độ việc làm cực kỳ cao bất thường so với quy mô dân số nhỏ bé của họ.
- **Nhóm "Thị trường Khổng lồ" (Như USA, India)**: Nằm xa về bên phải trục PC1, khẳng định đây là những nơi có khối lượng công việc lớn nhất thế giới.
- **Nhóm "Thị trường Mới nổi & Nhỏ"**: Tập trung ở góc dưới bên trái, cho thấy cả quy mô và mật độ đều ở mức thấp.

**Kết luận báo cáo**: Phân tích này giúp nhà đầu tư xác định được: Đâu là nơi để tìm kiếm số lượng (Volume - phía bên phải PC1) và đâu là nơi có môi trường cạnh tranh/sôi động cao nhất (Density - phía trên PC2).

### 5.2. PCA và Phân cụm KMeans Phân tích cấu trúc năng lực của phân khúc thị trường chuyên môn

Trong khi mục 5.1.1 cung cấp cái nhìn toàn cảnh về vị thế thị trường toàn cầu (quy mô và mật độ), mục này tập trung mổ xẻ **nhóm thu nhập cao** (lương > 140,000 USD).

Việc này giúp loại bỏ nhiễu từ các thị trường thu nhập thấp và tập trung xác định các nhân tố thực sự tạo nên sự khác biệt giữa các quốc gia hàng đầu.

<p align="center">
  <img src="https://github.com/user-attachments/assets/bd88f73e-627e-4f47-a0b1-3c795097b5db"
       width="975" height="715" />
</p>
<p align="center"><b>Hình 5.2.1: PCA phân tích cấu trúc năng lực của thị trường chuyên môn</b></p>
<br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/a19defb2-cbe1-4001-b667-7d5c577d67ac"
       width="975" height="715" />
</p>
<p align="center"><b>Hình 5.2.2: KMeans dựa trên PCA phân tích cấu trúc năng lực của thị trường chuyên môn</b></p>
<br>

#### Ý nghĩa các thành phần chính của PCA:

- **PC1 (53.9%)**: Đại diện cho **Độ phức tạp kỹ năng**. Tỷ lệ giải thích này cực cao cho thấy sự khác biệt giữa các nước giàu chủ yếu nằm ở số lượng kỹ năng (skill_count) và vai trò (role_count) yêu cầu trong mỗi công việc.
- **PC2 (24.1%)**: Đại diện cho **Quy mô dân số**, tách biệt các cường quốc đông dân khỏi các thị trường ngách.

#### Ý nghĩa K-means (Phân cụm chiến lược): Thuật toán tự động hóa việc chia nhóm các thị trường cao cấp dựa trên tọa độ năng lực:

- **Cụm dẫn đầu (như India)**: Nằm ở cực phải PC1, đại diện cho thị trường đòi hỏi kỹ năng đa dạng và chuyên sâu nhất.
- **Cụm thị trường ngách (như Afghanistan, Ukraine, Belgium)**: Nằm ở phía âm của PC1, cho thấy yêu cầu kỹ năng đặc thù hoặc ít phức tạp hơn so với trung bình nhóm cao cấp.
- **Cụm ổn định (như Switzerland, Netherlands, Singapore)**: Tập trung quanh trục 0, đại diện cho sự cân bằng giữa quy mô và năng lực.

---

## 🧪 6. DATA PIPELINE & XỬ LÝ DỮ LIỆU

### 6.1. Tổng quan pipeline

Pipeline dữ liệu trong đồ án được thiết kế theo hướng end-to-end, xử lý dữ liệu tuyển dụng từ dữ liệu thô (raw) đến dữ liệu sẵn sàng cho phân tích và lưu trữ trong database.

Luồng xử lý chính:

Raw data → Processing → Enrichment → ERD split → Database / Analysis

Pipeline được tổ chức thành các step độc lập, chạy tuần tự, cho phép:
- Tái chạy từng bước khi cần
- Kiểm soát lỗi và log theo từng giai đoạn
- Đảm bảo khả năng tái lập (reproducible)

### 6.2. STEP 0 – Reference & Seed Setup

**Mục tiêu:**  
Chuẩn bị các dữ liệu tham chiếu (reference) và mapping dùng xuyên suốt pipeline.

**Chức năng chính:**
- Chuẩn bị dữ liệu địa lý (GeoNames)
- Tạo bảng tham chiếu thành phố, quốc gia
- Xây dựng alias cho city name
- Chuẩn hóa các bảng mapping (skill, role, company size, currency, …)

**Đặc điểm:**
- Dữ liệu reference tách biệt khỏi dữ liệu job
- Không phụ thuộc vào dữ liệu crawl
- Dùng để join và enrich ở các step sau

### 6.3. STEP 1 – Crawling & Raw Data Collection

**Mục tiêu:**  
Thu thập dữ liệu tuyển dụng từ các nguồn công khai và API.

**Các bước chính:**
- Crawl dữ liệu từ API (public & authenticated)
- Thu thập dữ liệu ở dạng JSON / raw text
- Chuyển đổi JSON → CSV
- Quét nhanh text để phát hiện tín hiệu sơ bộ
- Gắn metadata nguồn dữ liệu

**Đặc điểm:**
- Không làm sạch sâu ở bước này
- Không suy diễn dữ liệu
- Giữ nguyên dữ liệu gốc để đảm bảo traceability


### 6.4. STEP 2 – Data Processing, Cleaning & Enrichment

Đây là core logic của toàn bộ pipeline.

### 6.4.0. Mapping Tool & Schema Alignment (Column Mapper App)

Trước khi thực hiện các bước extract, normalization và enrichment,
pipeline sử dụng **công cụ hỗ trợ mapping cột (Column Mapping App)** để đảm bảo
dữ liệu đầu vào từ các nguồn khác nhau được **chuẩn hóa về cùng một schema logic**.

**Chức năng chính của mapping tool:**
- Hiển thị toàn bộ các cột gốc từ từng nguồn dữ liệu
- Cho phép ánh xạ thủ công các cột nguồn → cột chuẩn của pipeline
- Kiểm tra và phát hiện:
  - Cột thiếu
  - Cột trùng nghĩa nhưng khác tên
  - Cột không cần thiết cho pipeline
- Đảm bảo các cột bắt buộc cho pipeline đều tồn tại trước khi xử lý tiếp

**Hỗ trợ mapping bán tự động (semi-automatic):**
- Tool tự động **gợi ý mapping cho khoảng 80–90% các cột phổ biến**
  dựa trên:
  - Tên cột
  - Pattern thường gặp
  - Mapping đã dùng trước đó
- Người dùng chỉ cần:
  - Xác nhận mapping đúng
  - Điều chỉnh các cột đặc thù hoặc không match
- Cách tiếp cận này giúp:
  - Giảm đáng kể thời gian mapping thủ công
  - Giữ được kiểm soát con người
  - Tránh sai lệch do tự động hoàn toàn

**Vai trò trong pipeline:**
- Là bước trung gian giữa raw/extracted data và processing logic
- Giảm rủi ro lỗi schema khi chạy pipeline tự động
- Tránh hard-code tên cột trong code xử lý
- Đảm bảo dữ liệu đầu vào tuân thủ chuẩn thiết kế (target schema)

Công cụ này được xây dựng dưới dạng **Streamlit app** và nằm trong thư mục `pipeline/tools/`,
được sử dụng khi:
- Thêm nguồn dữ liệu mới
- Thay đổi schema từ phía nhà cung cấp dữ liệu
- Kiểm tra nhanh tính tương thích của dữ liệu trước khi chạy pipeline đầy đủ

**Lưu ý:**
Ứng dụng Streamlit được phát triển để chạy trong môi trường nội bộ. Việc không triển khai public nhằm đảm bảo yêu cầu về mạng và bảo mật dữ liệu.

Sau khi mapping được xác nhận, dữ liệu được xử lý theo đúng thứ tự:
- **Mapping & Validation**: Kiểm tra file nguồn đã được mapping đầy đủ và schema thống nhất.
- **Extracting Description Signals**: Chỉ thực hiện sau khi mapping hoàn tất; trích xuất tín hiệu từ `job_description` khi dữ liệu gốc bị thiếu, không override.
- **Normalization & Enrichment**: Chuẩn hóa và làm giàu dữ liệu theo các rule-based mapping trước khi sang các bước tiếp theo.
<br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/ad0460db-c7d0-4dc7-9fe3-1085bb378f5c"
       width="975" height="715" />
</p>
<p align="center"><b>Hình 6.4.0.A: Ảnh minh họa app CSV Column Mapping Tool (ảnh 1)</b></p>
<br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/237d668d-228a-42d6-a3f2-fa2809630f18"
       width="975" height="715" />
</p>
<p align="center"><b>Hình 6.4.0.B: Ảnh minh họa app CSV Column Mapping Tool (ảnh 2)</b></p>
<br>

#### 6.4.1. Mapping & Validation
- Kiểm tra mapping tên cột (so sánh số lượng file gốc với file đã được mapping)
- Đảm bảo schema thống nhất giữa các nguồn
- Phát hiện và loại bỏ dữ liệu sai cấu trúc

#### 6.4.2. Extracting Description Signals
- Trích xuất tín hiệu từ job_description:
  - city
  - country
  - remote_option
  - salary / experience (nếu thiếu)
- Chỉ fill khi giá trị gốc bị thiếu (`__NA__`)
- Không override dữ liệu có sẵn

#### 6.4.3. Normalization
- Chuẩn hóa:
  - city
  - company
  - employment_type
  - currency
  - posted_date
- Áp dụng rule-based mapping
- Không dùng ML, không hard-code

#### 6.4.4. Enrichment
- Suy ra country từ city
- Enrich skill level & skill category
- Chuẩn hóa role name về tập role chuẩn

#### 6.4.5. Validation
- Kiểm tra dữ liệu lương và kinh nghiệm
- Loại bỏ các giá trị không hợp lệ
- Giữ nguyên các giá trị thiếu (không suy đoán)

#### 6.4.6. Combining & ERD Splitting
- Gộp dữ liệu đã xử lý
- Tách dữ liệu theo mô hình ERD:
  - job_postings
  - companies
  - skills
  - locations
  - các bảng quan hệ N–N

### 6.5. STEP 3 – Database Upload

**Mục tiêu:**  
Đưa dữ liệu đã xử lý vào hệ quản trị cơ sở dữ liệu.

**Hỗ trợ:**
- Xuất SQL INSERT statements (backup)
- Load dữ liệu vào PostgreSQL local
- Load dữ liệu lên Supabase Cloud

<p align="center">
  <img src="https://github.com/user-attachments/assets/a850cd47-043c-4e1e-bf69-52ad7189caf0"
       width="975" height="715" />
</p>
<p align="center"><b>Hình 6.5: Data sau khi upload lên cloud Supabase</b></p>
<br>

**Đặc điểm:**
- Load theo thứ tự bảng cha → bảng con
- Đảm bảo toàn vẹn khóa ngoại
- Có thể chạy độc lập với pipeline xử lý

### 6.6. Nguyên tắc thiết kế pipeline

Pipeline được xây dựng theo các nguyên tắc:

- **Reproducible:** Có thể chạy lại toàn bộ pipeline từ raw data  
- **No hard-code:** Mọi mapping đều thông qua bảng reference  
- **Traceable:** Giữ metadata nguồn dữ liệu xuyên suốt pipeline  
- **Modular:** Mỗi step là một module độc lập

---

## 🗄️ 7. DATABASE DESIGN & ERD

### 7.1. Vai trò của Database trong hệ thống

Database **không trực tiếp tham gia vào các bước xử lý dữ liệu**
(cleaning, normalization, enrichment),
nhưng đóng vai trò là **chuẩn thiết kế (target schema)** cho toàn bộ pipeline.

Cấu trúc ERD được xác định **trước khi xây dựng pipeline** và được sử dụng làm cơ sở cho:
- Mapping cột dữ liệu
- Chuẩn hóa giá trị
- Enrichment
- Tách bảng dữ liệu

Dữ liệu đầu ra của pipeline luôn đảm bảo **tương thích hoàn toàn với schema database**
trước khi được load vào PostgreSQL.

Sau khi pipeline hoàn tất, **database trở thành nguồn dữ liệu chính (single source of truth)** để:
- Truy xuất dữ liệu cho các bước phân tích (EDA, PCA, KMeans)
- Cung cấp dữ liệu chuẩn cho Power BI dashboard
- Kết nối với Streamlit app để hiển thị và khai thác insight

Toàn bộ phân tích và dashboard **chỉ sử dụng dữ liệu đã được load vào database**,
đảm bảo tính nhất quán giữa pipeline, phân tích và hiển thị kết quả.

### 7.2. ERD (Entity Relationship Diagram)

<p align="center">
  <img src="https://github.com/user-attachments/assets/8e7fde83-e7d1-4a1e-8132-ddddb61e0cf3"
       width="975" height="715" />
</p>
<p align="center"><b>Hình 7.2: Mô hình ERD</b></p>
<br>

ERD mô tả:
- Các bảng chính trong hệ thống
- Quan hệ giữa các bảng
- Khóa chính, khóa ngoại
- Các bảng trung gian (many-to-many)

### 7.3. Database Schema & Query

Thư mục `database/` bao gồm:
- File SQL tạo bảng theo ERD
- Các index phục vụ truy vấn
- Một số query mẫu để:
  - Gộp dữ liệu phân tích
  - Truy vấn lương
  - Truy vấn remote / onsite
  - Phục vụ dashboard và phân tích

Database được sử dụng cho:
- Lưu trữ dữ liệu đã chuẩn hóa
- Truy vấn phân tích
- Kết nối Power BI và Streamlit dashboard

---

## 8. 📁 Project Folder Structure
Thư mục dự án được tổ chức theo hướng **tách biệt rõ ràng giữa Data Engineering,
Data Analysis và Visualization**, giúp pipeline dễ bảo trì, mở rộng và tái sử dụng.

## 📁 Project Folder Structure
```bash
data_industry_insights/
│
├── app/                                            # STREAMLIT UI / GIAO DIỆN STREAMLIT
│   ├── assets/                                     # Static assets (CSS, images, icons) / Tài nguyên tĩnh (CSS, hình ảnh, icon)
│   ├── pages/                                      # Multi-page Streamlit views / Các trang giao diện Streamlit
│   └── app.py                                      # Streamlit app entry point / File khởi chạy ứng dụng Streamlit
│
├── analysis/                                       # EXPLORATORY ANALYSIS & MANUAL VALIDATION / PHÂN TÍCH KHÁM PHÁ VÀ KIỂM TRA DỮ LIỆU THỦ CÔNG
│   ├── data/                                       # Analysis-specific datasets / Dữ liệu dùng riêng cho phân tích
│   ├── 1_dataset_construction.py                   # Dataset inspection & construction (output to analysis/data/) / Kiểm tra và xây dựng tập dữ liệu phân tích 
│   ├── 2_analysis_EDA_PCA_50k.ipynb                # EDA & PCA on sampled data (50K rows) / EDA & PCA trên tập mẫu 50K
│   ├── 3_analysis_EDA_PCA_500k.ipynb               # EDA & PCA on full dataset (~500K rows) / EDA & PCA trên tập đầy đủ (~500K)
│   └── 4_splitting_tables_from_analysis.py         # Split filtered analysis data into ERD tables (output to dashboard/data/) / Tách dữ liệu đã lọc từ analysis thành các bảng theo ERD
│
├── dashboard/                                      # POWER BI DASHBOARD & REPORTS / DASHBOARD VÀ BÁO CÁO POWER BI
│   ├── data/                                       # Curated datasets for dashboard (from analysis 500K) / Dữ liệu chọn lọc cho dashboard (từ PCA 500K)
│   └── Data_Industry_Insights.pbix                 # Power BI report file / File báo cáo Power BI
│
├── database/                                       # DATABASE SCHEMA & ERD (STRUCTURE ONLY) / SCHEMA VÀ MÔ HÌNH ERD (CHỈ CHỨA CẤU TRÚC)
│   ├── queries/                                    # SQL queries for analysis & validation / Câu lệnh SQL phục vụ phân tích và kiểm tra 
│   ├── schema.sql                                  # Database schema (DDL) / File tạo bảng và ràng buộc database
│   ├── ERD.png                                     # Entity Relationship Diagram / Sơ đồ quan hệ thực thể (ERD)
│   └── README.md                                   # Database structure and usage notes / Giải thích cấu trúc và cách dùng database
│
├── data/                                           # DATA FILES ONLY / FOLDER CHỈ CHỨA DATA (JSON VÀ CSV SAU KHI LẤY TỪ PIPELINE)
│   ├── data_raw/                                   # Raw scraped data (API / HTML / JSON) / Dữ liệu thô (file JSON lấy trực tiếp từ web)
│   └── data_processing/                            # Transformed intermediate data / Dữ liệu chuyển đổi (file CSV sau khi parse từ JSON)
│   │   ├── data_extracted/                         # Extracted raw fields / Dữ liệu trích xuất trực tiếp từ JSON
│   │   ├── data_mapped/                            # Mapped & standardized data / Dữ liệu đã map và chuẩn hóa cột
│   │   └── data_enriched/                          # After augmentation & derivation / Dữ liệu đã được làm giàu (bổ sung, suy diễn thêm thuộc tính)
│   │ 
│   ├── data_processed/                             # Cleaned final data for analytics / Dữ liệu cuối để phân tích (đã merge và tách bảng)
|   |
|   ├── data_reference/                             # Reference & dimension data / Dữ liệu tham chiếu và dimension
|   |   ├── geonames_raw/                           # Raw GeoNames reference data / Dữ liệu địa lý gốc từ GeoNames
|   |   ├── cities.csv                              # City reference table / Bảng tham chiếu thành phố (tọa độ, dân số)
|   |   ├── city_alias_reference.csv                # City alias mapping / Ánh xạ alias tên thành phố
|   |   ├── countries.csv                           # Country reference table / Bảng tham chiếu quốc gia (ISO, tên)
|   |   ├── company_size_mapping.csv                # Company size mapping / Mapping quy mô công ty
|   |   ├── currency_mapping.csv                    # Currency normalization mapping / Mapping chuẩn hóa tiền tệ
|   |   ├── education_level_mapping.csv             # Education level mapping / Mapping trình độ học vấn
|   |   ├── employment_type_mapping.csv             # Employment type mapping / Mapping loại hình làm việc
|   |   ├── industry_mapping.csv                    # Industry mapping / Mapping ngành nghề
|   |   ├── job_level_mapping.csv                   # Job level mapping / Mapping cấp độ nghề nghiệp
|   |   ├── role_names_mapping.csv                  # Role name standardization / Mapping chuẩn hóa tên chức danh
|   |   ├── skill_mapping.csv                       # Skill name mapping / Mapping chuẩn hóa tên kỹ năng
|   |   ├── skill_level_mapping.csv                 # Skill level mapping / Mapping mức độ kỹ năng
|   |   └── unmatched_city_country.csv              # Unmatched geo values log / Log city–country không match
│   │
│   ├── data_seeds/                                 # Lookup & reference data / Dữ liệu chuẩn tra cứu (không dùng cho pipeline chính)
│   └── metadata/                                   # Schema & source documentation / Tài liệu mô tả cấu trúc JSON của từng nguồn web
│
├── pipeline/                                       # DATA PIPELINE LOGIC / LOGIC XỬ LÝ DỮ LIỆU (FOLDER CHỈ CHỨA CODE PYTHON)
│   ├── step0_seeds/                                # Seed & reference preparation / Chuẩn bị dữ liệu seed và dữ liệu tham chiếu
│   │   ├── 0.0_build_seed_data.py                  # Build initial seed datasets (not used in pipeline) / Tạo dữ liệu ban đầu để tham khảo tên cột, ko dùng trong pipeline
│   │   ├── 0.1_setup_geonames_reference.py         # Setup GeoNames-based geo reference / Chuẩn bị dữ liệu địa lý chuẩn từ GeoNames
│   │   ├── 0.2_build_city_alias_reference.py       # Build city alias mapping / Tạo bảng ánh xạ alias cho tên thành phố
│   │   └── 0.3_build_geo_reference.py              # Build unified geo reference / Hợp nhất dữ liệu địa lý thành reference cuối
│   │
│   ├── step1_crawlers/                             # Data collection via APIs (and experiments) / Thu thập dữ liệu qua API (và thử nghiệm)
│   │   ├── api/                                    # API-based data collection / Thu thập dữ liệu qua API
│   │   │   ├── authenticated/                      # Authenticated APIs (require API keys) / API cần xác thực
│   │   │   └── public/                             # Public APIs / API công khai
│   │   │
│   │   ├── scrape/                                 # HTML web scraping - Experimental scraping attempts (not used in final pipeline) / Đã tetst nhưng data rác ko dùng được
│   │   │   └── protected/                          # Anti-bot sites (testing only) / Web có chống bot
│   │   │
│   │   ├── 1.1_run_step1_full_clawlers.py          # Central crawler entry point invoking site-specific crawlers / Điểm vào trung tâm gọi các crawler riêng cho từng website
│   │   └── 1.2_dataset_hugging.py                  # Hugging Face dataset downloader (primary data source) / Tải dataset từ Hugging Face (nguồn dữ liệu trực tiếp)
│   │
│   ├── step2_processing/                           # Data cleaning, normalization & enrichment / Làm sạch, chuẩn hóa và làm giàu dữ liệu
│   │   ├── 2.1_mapping_check.py                    # Validate column mapping / Kiểm tra và xác nhận mapping tên cột
│   │   ├── 2.2_extracting_description_signals.py   # Extract signals from job descriptions / Trích xuất tín hiệu từ mô tả công việc
│   │   ├── 2.3_normalizing_values.py               # Normalize categorical values / Chuẩn hóa giá trị danh mục (city, company, type, currency, ...)
│   │   ├── 2.4_enriching_country_from_city.py      # Enrich country data from city / Suy ra quốc gia từ thông tin thành phố
│   │   ├── 2.5_enriching_skill_level_category.py   # Enrich skill level & category / Làm giàu cấp độ và phân loại kỹ năng
│   │   ├── 2.6_standardizing_role_name.py          # Standardize job role names / Chuẩn hóa tên chức danh công việc
│   │   ├── 2.7_validating_salary_exp.py            # Validate salary & experience fields / Kiểm tra và làm sạch dữ liệu lương và kinh nghiệm
│   │   ├── 2.8_combining_data.py                   # Combine processed datasets / Gộp dữ liệu sau xử lý
│   │   ├── 2.9_splitting_tables_erd.py             # Split data into ERD tables / Tách dữ liệu theo cấu trúc ERD
│   │   └── 2.10_run_step2_full_pipeline.py         # Run full STEP 2 pipeline / Chạy toàn bộ pipeline STEP 2
│   │
│   ├── step3_database_upload                       # Load processed data into databases / Đưa dữ liệu đã xử lý vào database
│   │   ├── 3.0_export_csv_to_postgresql.py         # Export SQL INSERT statements (for backup) / Xuất file SQL chứa câu lệnh INSERT dữ liệu (lưu backup)
│   │   ├── 3.1_loading_data_to_local_postgre.py    # Load data into local PostgreSQL / Nạp dữ liệu vào PostgreSQL local
│   │   └── 3.2_loading_data_to_supabase.py         # Load data into Supabase Cloud PostgreSQL / Nạp dữ liệu lên Supabase Cloud
│   │
│   ├── tools/                                      # Helper tools for data processing / Công cụ hỗ trợ chạy thủ công
│   │   └── column_mapper_app.py                    # Column mapping and normalization tool / App hỗ trợ map và kiểm tra tên cột
│   │
│   └── pipeline_app.py                             # Pipeline entry point / File chạy chính
│ 
├── .env                                    
├── .gitignore                  
├── README.md
└── requirements.txt
```

- **app/**  
  Chứa ứng dụng Streamlit phục vụ hiển thị insight, dashboard tương tác và kết nối
  trực tiếp với database sau khi pipeline hoàn tất.

- **analysis/**  
  Chứa notebook phân tích EDA, PCA và KMeans trên các tập dữ liệu đã chuẩn hóa  
  (sample 50K và full ~500K), phục vụ nghiên cứu và báo cáo.

- **dashboard/**  
  Chứa Power BI dashboard, sử dụng dữ liệu đã xử lý và tổng hợp để trực quan hóa
  thị trường lao động ngành Data.

- **database/**  
  Chứa thiết kế database bao gồm:
  - ERD (Entity Relationship Diagram)
  - Schema SQL
  - Các câu truy vấn mẫu phục vụ phân tích và dashboard  
  Database đóng vai trò là chuẩn thiết kế (target schema) và nguồn dữ liệu chính
  sau pipeline.

- **data_seeds/**  
  Dữ liệu seed và lookup ban đầu, dùng để:
  - Chuẩn hóa giá trị danh mục
  - Kiểm tra mapping
  - Định nghĩa chuẩn dữ liệu  
  (không phải dữ liệu job thực tế)

- **data_unmatched_report/**  
  Chứa các báo cáo log những giá trị **không match được** trong quá trình pipeline  
  (ví dụ: city–country không xác định, skill không map được).  
  Thư mục này dùng để **kiểm tra các mapping còn thiếu và bổ sung lại vào các file mapping**,  
  nhằm hỗ trợ quá trình **extract và enrichment đạt độ bao phủ tối đa**,  
  **không dùng cho phân tích hay modeling**.

- **metadata/**  
  Chứa metadata mô tả nguồn dữ liệu, schema JSON gốc và thông tin kỹ thuật của từng
  nguồn crawl/API, phục vụ traceability và debug pipeline.
- **pipeline/**  
  Chứa toàn bộ logic xử lý dữ liệu, được thiết kế theo hướng modular:
  - **step0_seeds/**: Chuẩn bị seed và dữ liệu tham chiếu  
  - **step1_crawlers/**: Thu thập dữ liệu từ các nguồn tuyển dụng  
  - **step2_processing/**: Làm sạch, chuẩn hóa và enrichment dữ liệu  
  - **step3_database_upload/**: Load dữ liệu đã chuẩn hóa vào PostgreSQL

- **.env**  
  Biến môi trường cho database, API key và cấu hình bảo mật.

- **requirements.txt**  
  Danh sách thư viện Python cần thiết để chạy pipeline và ứng dụng.

Cấu trúc này đảm bảo:
- Pipeline có thể chạy lại (reproducible)
- Dễ mở rộng thêm nguồn dữ liệu hoặc bước xử lý mới
- Phân tách rõ ràng giữa xử lý dữ liệu, phân tích và hiển thị

---

## ⚙️ 9. INSTALLATION & SETUP

### 9.1. Yêu cầu hệ thống
- Python >= 3.9
- PostgreSQL (local) **hoặc** Supabase PostgreSQL
- Git
- (Tuỳ chọn) Power BI Desktop
- (Tuỳ chọn) Streamlit

### 9.2. Clone project
```bash
git clone https://github.com/<your-username>/data_industry_insights.git
cd data_industry_insights
```

### 9.3. Tạo môi trường ảo và cài thư viện
```
python -m venv .venv
source .venv/bin/activate        # Linux / Mac
.venv\Scripts\activate           # Windows
```

```
pip install -r requirements.txt
```

### 9.4. Cấu hình biến môi trường
```
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=data_industry_insights
DB_USER=postgres
DB_PASSWORD=your_password

# Supabase (optional)
SUPABASE_HOST=...
SUPABASE_PORT=...
SUPABASE_DB=...
SUPABASE_USER=...
SUPABASE_PASSWORD=...

# API keys (if any)
API_KEY_1=...
```

### 9.5. Chạy pipeline dữ liệu

#### STEP 0 – Chuẩn bị reference & seed
```
python pipeline/step0_seeds/0.1_setup_geonames_reference.py
python pipeline/step0_seeds/0.2_build_city_alias_reference.py
python pipeline/step0_seeds/0.3_build_geo_reference.py
```

#### STEP 1 – Crawling dữ liệu
```
python pipeline/step1_crawlers/run_step1.py
1.1_run_step1_full_clawlers.py
1.2_dataset_hugging.py
```

#### STEP 3 – Load dữ liệu vào database
```
python pipeline/step3_database_upload/3.1_loading_data_to_local_postgre.py
# hoặc
python pipeline/step3_database_upload/3.2_loading_data_to_supabase.py
```

### 9.6. Run Analysis & Dashboard

**Jupyter Notebook**
```bash
jupyter notebook analysis/
```

**Streamlit Application**
```bash
streamlit run app/app.py
```

After pipeline completion, the database becomes the **single source of truth** for analysis, dashboarding and application usage.

---

## 🧩 10. GIỚI HẠN CỦA ĐỒ ÁN (LIMITATIONS)

Mặc dù tập dữ liệu và pipeline được xây dựng theo hướng chuẩn hóa và có khả năng tái lập, đồ án vẫn tồn tại một số giới hạn khách quan:

- **Giới hạn dữ liệu lịch sử**:  
  Dữ liệu tuyển dụng công khai đầy đủ cho giai đoạn trước năm 2023 rất hạn chế.  
  Phần lớn dữ liệu lịch sử chất lượng cao (2020–2022) thuộc các **paid datasets**, do đó không được sử dụng trong đồ án này.

- **Thiên lệch theo khu vực**:  
  Các thị trường như Hoa Kỳ, châu Âu có độ phủ dữ liệu cao hơn so với các thị trường nhỏ hoặc mới nổi.  
  Nguyên nhân chủ yếu đến từ **sự khác biệt trong chính sách công khai dữ liệu và mức độ minh bạch thông tin tuyển dụng**.  
  Các quốc gia phương Tây có khung pháp lý và hạ tầng dữ liệu mở hơn, cho phép công bố rộng rãi thông tin việc làm, trong khi nhiều khu vực khác hạn chế chia sẻ dữ liệu hoặc yêu cầu trả phí để truy cập.

- **Thiếu minh bạch về lương**:  
  Tại một số quốc gia, thông tin lương không được công khai đầy đủ và thường ở dạng thỏa thuận, dẫn đến tỷ lệ giá trị thiếu (missing) cao ở các trường `min_salary`, `max_salary`.

- **Rule-based processing**:  
  Pipeline sử dụng hoàn toàn các luật (rule-based) và bảng mapping, không áp dụng Machine Learning cho việc suy đoán dữ liệu, nhằm tránh việc tạo ra giá trị giả (hallucinated data).  
  Điều này giúp đảm bảo độ tin cậy nhưng có thể làm giảm độ bao phủ trong một số trường hợp đặc biệt.

Các giới hạn trên được xem là **đặc điểm của dữ liệu và bối cảnh thu thập**, không phải lỗi trong quá trình xử lý hay thiết kế pipeline.

---

## 🚀 11. HƯỚNG PHÁT TRIỂN & MỞ RỘNG (FUTURE WORK)

Trong tương lai, đồ án có thể được mở rộng theo các hướng sau:

- **Mở rộng nguồn dữ liệu**:  
  Tích hợp thêm các **paid datasets (mua dữ liệu)** hoặc hợp tác với các đối tác cung cấp dữ liệu tuyển dụng nhằm cải thiện độ phủ cho giai đoạn lịch sử (2020–2022), đặc biệt đối với các thị trường và thời kỳ không có dữ liệu công khai.

- **Kết hợp Machine Learning có kiểm soát**:  
  Áp dụng Machine Learning **chỉ cho mục đích phân tích và dự đoán xu hướng tương lai**, ví dụ:
  - Phân tích xu hướng vai trò và kỹ năng (role & skill trend analysis)

  **Machine Learning không được sử dụng để fill hoặc thay thế dữ liệu gốc** trong pipeline xử lý.  
  Các kết quả ML (nếu có) chỉ mang tính tham khảo, có thể đi kèm confidence score và **không override dữ liệu thực tế đã thu thập**.

---

## 🏁 KẾT LUẬN

Đồ án **Data Industry Insights** đã xây dựng và triển khai một quy trình phân tích dữ liệu thị trường lao động ngành Data theo hướng **end-to-end**, bao gồm thu thập dữ liệu, xử lý – chuẩn hóa – enrichment theo pipeline rule-based, và các bước phân tích khám phá dữ liệu (EDA), PCA và KMeans.

Thông qua các phương pháp phân tích được áp dụng, đồ án cung cấp một số góc nhìn tổng quan về:
- Quy mô và mức độ phân bố của thị trường lao động ngành Data  
- Sự khác biệt giữa các khu vực về vai trò, kỹ năng và hình thức làm việc  
- Cấu trúc và phân khúc thị trường dựa trên các đặc trưng quan sát được từ dữ liệu  

Kết quả phân tích phản ánh **xu hướng và đặc điểm của tập dữ liệu được thu thập**, đặc biệt trong giai đoạn **2023–2025**, và không nhằm khái quát hóa tuyệt đối cho toàn bộ thị trường lao động toàn cầu.

Pipeline xử lý dữ liệu được thiết kế theo hướng **rule-based, reproducible và bám sát schema database (ERD)**, giúp đảm bảo tính nhất quán giữa dữ liệu đầu vào, dữ liệu phân tích và dữ liệu phục vụ dashboard. Tuy nhiên, pipeline và các kết quả phân tích vẫn phụ thuộc vào phạm vi và chất lượng của nguồn dữ liệu công khai, đặc biệt đối với dữ liệu lịch sử.

Nhìn chung, đồ án đóng vai trò như một **bài toán nghiên cứu và thực hành kỹ thuật dữ liệu**, minh họa cách xây dựng pipeline, tổ chức dữ liệu và áp dụng các phương pháp phân tích để khám phá thị trường lao động ngành Data, đồng thời tạo nền tảng cho các hướng mở rộng và cải thiện trong tương lai.
