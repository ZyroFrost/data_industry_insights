# Data Industry Insights
## Overview
End-to-end data pipeline and analytics project analyzing global Data job market trends (2020–2025), with web crawlers, cleaned datasets, Power BI dashboard, and Streamlit insights app.

## 📁 Project Folder Structure
```
data_industry_insights/
├── app/                        # Streamlit UI
├── analysis/                   # EDA, PCA, clustering (50K & 500K)
├── dashboard/                  # Power BI dashboard
├── database/                   # Database schema & ERD
├── data/
│   ├── data_raw/               # Raw collected data
│   ├── data_processing/        # Intermediate processed data
│   ├── data_processed/         # Final analytics-ready data
│   └── data_reference/         # Reference & mapping tables
├── pipeline/
│   ├── step0_seeds/            # Seed & reference preparation
│   ├── step1_crawlers/         # API data collection
│   ├── step2_processing/       # Cleaning, normalization & enrichment
│   └── step3_database_upload   # Load data into PostgreSQL
├── .env
├── requirements.txt
└── README.md
```

## 3. Tổng hợp kết quả 7 bước EDA cơ bản

Quá trình Phân tích Khám phá Dữ liệu (EDA) được thực hiện trên tập dữ liệu khoảng **513.000 dòng** nhằm mục đích hiểu rõ cấu trúc thị trường lao động ngành dữ liệu toàn cầu.

### Bước 1: Thu thập và Tổng quan (Data Overview)
Dữ liệu bao gồm các thông tin về:
- Quốc gia  
- Chức danh  
- Kỹ năng  
- Hình thức làm việc  
- Kinh nghiệm  
- Lương (USD)

Phân tích tập trung vào các biến chính ảnh hưởng đến **thu nhập** và **nhu cầu tuyển dụng**.

---

### Bước 2: Kiểm tra dữ liệu khuyết thiếu (Data Integrity)
- Xác định các khoảng trống dữ liệu (data gap) trong giai đoạn **2021–2022**.  
- Phát hiện sự thiếu minh bạch về lương tại một số thị trường như **Ấn Độ, Pháp và Ý**, nơi mức lương thường được thỏa thuận riêng.

---

### Bước 3: Làm sạch và Chuẩn hóa (Data Cleaning)
- Quy đổi tất cả đơn vị tiền tệ về **USD** để đảm bảo tính đồng nhất.  
- Nhóm các chức danh công việc vào **10 nhóm chính**  
  (ví dụ: Data Engineer, Data Analyst, Data Scientist).

---

### Bước 4: Thống kê mô tả (Descriptive Statistics)
- **Lương trung vị (Median):** 104,022.06 USD  
- **Lương trung bình (Average):** 143,098.19 USD  

Kết luận: Dữ liệu có phân phối **lệch phải (right-skewed)** do ảnh hưởng của các mức lương cao tại thị trường Mỹ.

---

### Bước 5: Phân tích đơn biến
- **Thị trường:** Mỹ dẫn đầu với **37.3%** thị phần bài đăng tuyển dụng.  
- **Kỹ năng:** SQL (**23.2%**) và Python (**22.8%**) là hai kỹ năng “phải có”.  
- **Hình thức làm việc:** **87.9%** công việc yêu cầu làm việc tại văn phòng (Onsite).

---

### Bước 6: Phân tích mối tương quan giữa các biến
- **Kinh nghiệm vs Lương:** Tồn tại tương quan thuận mạnh mẽ (kinh nghiệm tăng thì lương tăng).  
- **Thời gian:** Thị trường có xu hướng chuyển dịch từ nhóm đặc thù (2023) sang thị trường đại chúng (2025).

---

### Bước 7: Phát hiện ngoại lệ (Outliers)
- Sử dụng **Boxplot** để xác định các mức lương trên **300,000 USD** là ngoại lệ.  
- Các ngoại lệ chủ yếu rơi vào các vai trò **chuyên gia cao cấp** hoặc **lãnh đạo** tại thị trường Mỹ.

---

## 4. Kết quả phân tích gom cụm (K-Means)

Dựa trên các đặc trưng thị trường như **quy mô**, **kỹ năng** và **hình thức làm việc**, thuật toán **K-Means** được áp dụng để phân nhóm các quốc gia.

### 4.1. Xác định số cụm tối ưu (Elbow Method)
Phương pháp **Elbow** được sử dụng để lựa chọn số cụm tối ưu.  
Kết quả cho thấy **K = 4** là điểm mà tổng bình phương sai lệch trong cụm giảm ổn định, giúp phân loại thị trường rõ rệt nhất.

---

### 4.2. Trực quan hóa các cụm trên không gian PCA
Do dữ liệu có nhiều chiều, **PCA** được sử dụng để giảm chiều và trực quan hóa trên không gian **2D** và **3D**.

#### A. Biểu đồ cụm 2D (PC1 vs PC2)
Giúp quan sát sự phân hóa giữa nhóm thị trường khổng lồ (Mỹ) và các nhóm thị trường truyền thống hoặc linh hoạt.

> **[Hình 4.2.a – Chèn biểu đồ PCA 2D tại đây]**

#### B. Biểu đồ cụm 3D
Cung cấp cái nhìn sâu hơn về sự phân tách của các cụm khi bổ sung thêm chiều về sự đa dạng vai trò công việc.

> **[Hình 4.2.b – Chèn biểu đồ PCA 3D tại đây]**

---

### 4.3. Đặc điểm các cụm
- **Cụm 0 (Thị trường dẫn đầu):** Quy mô cực lớn, đa dạng kỹ năng (USA).  
- **Cụm 1 (Thị trường Onsite):** Tỷ lệ làm việc toàn thời gian và tại văn phòng cao (Châu Âu).  
- **Cụm 2 (Thị trường linh hoạt):** Ưu tiên Remote, quy mô vừa (Startup).  
- **Cụm 3 (Thị trường chuyên biệt):** Ít bài đăng hơn nhưng yêu cầu kỹ năng rất cao (Singapore, Mexico).

---

## 5. Kết quả phân tích PCA và K-Means trên các thành phần chính (PC)

### 5.1. Kết quả của PCA (Principal Component Analysis)

#### 5.1.1. PCA và phân cụm K-Means phân tích thị trường toàn cầu

> **[Hình 5.1.1 – Biểu đồ PCA phân tích vị thế thị trường toàn cầu]**  
> **[Hình 5.1.2 – Biểu đồ K-Means phân khúc thị trường toàn cầu]**  
> **[Hình 5.1.3 – Phân tích đặc trưng các cụm sau K-Means]**

- **PC1 (47.19%)**: Đại diện cho **quy mô** (số lượng việc làm và dân số càng lớn thì càng nằm về bên phải).  
- **PC2 (37.61%)**: Đại diện cho **mật độ/độ sôi động** (mật độ việc làm càng cao thì càng nằm phía trên).

Thuật toán **K-Means** tự động nhóm các quốc gia có đặc điểm tương đồng thành **4 phân khúc chiến lược**:  
Khổng lồ, Sôi động, Đang phát triển và Nhỏ.

**Ý nghĩa kết quả:**
- Nhóm *Outliers đặc biệt* (ví dụ: Kiribati): Nằm tách biệt phía trên trục PC2, thể hiện mật độ việc làm bất thường so với quy mô dân số nhỏ.  
- Nhóm *Thị trường khổng lồ* (USA, India): Nằm xa về bên phải trục PC1, khẳng định khối lượng việc làm lớn nhất.  
- Nhóm *Thị trường mới nổi & nhỏ*: Tập trung ở góc dưới bên trái, cho thấy cả quy mô và mật độ đều thấp.

**Kết luận:**  
Phân tích này giúp xác định:
- Khu vực có **số lượng cơ hội lớn** (phía bên phải PC1).  
- Khu vực có **mức độ cạnh tranh và sôi động cao** (phía trên PC2).

---

#### 5.1.2. PCA và phân cụm K-Means phân tích cấu trúc năng lực của phân khúc thị trường chuyên môn

Trong khi mục 5.1.1 cung cấp cái nhìn toàn cảnh về vị thế thị trường toàn cầu (quy mô và mật độ), mục này tập trung mổ xẻ **nhóm thu nhập cao** (lương > 140,000 USD).

Việc này giúp loại bỏ nhiễu từ các thị trường thu nhập thấp và tập trung xác định các nhân tố thực sự tạo nên sự khác biệt giữa các quốc gia hàng đầu.

> **[Hình 5.2.1 – Biểu đồ PCA phân tích cấu trúc năng lực thị trường chuyên môn]**  
> **[Hình 5.2.2 – Chèn hình tại đây]**