# Supermarket Business Intelligence & Spatial Analytics Pipeline

An end-to-end commercial analytics framework combining BigQuery SQL engineering, spatial geolocation filtering, econometric regression modeling, and multi-tier Looker Studio dashboards. Developed to extract operational efficiencies, mitigate customer churn, optimize store staffing, and pinpoint high-yield retail expansion targets across Israel.

---

## Executive Summary & Key Results

Retail analytics often suffers from biased telemetry caused by unfiltered staff presence, GPS noise, and unidentified foot traffic. This project establishes an enterprise **Single Source of Truth (SSOT)** for supermarket operational metrics by parsing raw geolocation logs, transaction registers, national demographic statistics, and competitor locations.

### Key Performance Impact & Findings

* **Spatial Expansion Modeling:** Identified **Modi'in Illit** as the top retail expansion market with an **Opportunity Score of 91.8k** (91,000 residents with zero major discount chain competitors).
* **Econometric Dwell-Time Proof:** Quantified the financial impact of customer store duration using linear regression ($R^2 = 0.7593, p < 0.001$). Every additional minute of customer dwell time yields an average basket increase of **+7.62 NIS**.
* **Churn & Foot-Traffic Alert:** Uncovered a statistically significant weekly foot-traffic decline of **-12.89 visitors per week** ($R^2 = 0.2036, p = 0.018$), triggering a targeted loyalty retention mandate.
* **Operational Bottleneck Discovery:** Identified the "Thursday Effect" where average basket sizes surge by **+55%** (from ~380 NIS to ~590 NIS), coupled with staffing gaps at peak hours (14:00) and delivery supply chain disruptions on key dates.

| Analytics Dimension | Core Metric / Formula | Key Finding / Business Value | Operational Recommendation |
| :--- | :--- | :--- | :--- |
| **Dwell Time Value** | $\text{Spend} = 25.65 + 7.62 \times \text{DwellMinutes}$ | Strong positive correlation ($R^2 = 0.76$); 60 min stay averages ~607 NIS vs 15 min at ~75 NIS | Enhance in-store experience (climate control, layout pacing, mini-cafe) |
| **Weekly Traffic Trend** | $\text{Traffic} = 1599.04 - 12.89 \times \text{WeekNum}$ | Sustained loss of ~13 unique shoppers/week ($p = 0.018$) | Launch geo-targeted loyalty campaigns and win-back offers |
| **Expansion Opportunity** | $\text{Score} = \frac{\text{Population}}{\max(\text{Competitors}, 1)}$ | Modi'in Illit ranks #1 (91.8k Score); Tel Aviv & Jerusalem highly saturated | Prioritize flagship branch in Modi'in Illit |
| **Digital Loyalty Mix** | $\frac{\text{App Repeat Users}}{\text{Total Transactions}}$ | 87% of revenue driven by Repeat App users; 3,596 Walk-in shoppers untracked | Launch aggressive app onboarding for Walk-in shoppers |

---

## Tech Stack & Data Architecture

```
[Raw Telemetry & Logs]
   ├── Geolocation Pings (BigQuery)
   ├── POS Sales Transactions (log_sales)
   ├── CBS Demographics (Lamas_israel_2025)
   └── Retail Competitor Database (supermarkets_il)
            │
            ▼
[Phase 1: Spatial & Noise Cleaning]
   ├── Filter Accuracy (accuracy_m <= 30)
   ├── Dwell-Time Outlier Truncation (1..180 mins)
   └── Algorithmic Staff Role Categorization (CTE Windowing)
            │
            ▼
[Phase 2: Econometric & Behavioral Modeling]
   ├── Dwell Time vs. Revenue Linear Regression
   ├── Weekly Customer Churn Trend Analysis
   └── Customer Lifetime & VIP Segmentation
            │
            ▼
[Phase 3: Production BI Datasets]
   ├── Opportunity Score Calculation (Population / Competitors)
   ├── Staffing & Traffic Heatmap Pipeline
   └── Dashboard Master View (LEFT JOIN App & Walk-in Telemetry)
            │
            ▼
[Looker Studio 3-Tier Enterprise Dashboard]
   ├── Executive Overview (CEO / Board)
   ├── Operations & Efficiency (Store Manager)
   └── Strategy & Expansion (VP Business Development)
```

* **Data Warehouse & Query Engine:** Google BigQuery (Standard SQL, CTEs, Window Functions, Spatial Aggregations).
* **Statistical Modeling & Python:** Python 3.12, Pandas, NumPy, SciPy (Ordinary Least Squares Regression, Hypothesis Testing).
* **Business Intelligence:** Looker Studio (Multi-page interactive dashboards, dynamic filtering, custom calculated fields).
* **Data Sources:** Store POS transaction logs (`log_sales`), GPS geolocation telemetry (`geolocation`), Central Bureau of Statistics (`Lamas_israel_2025`), and Israel Supermarkets Directory (`supermarkets_il`).

---

## Data Pipeline & SQL Engineering

### 1. Telemetry Cleaning & Automated Staff Classification

Raw geolocation logs include non-customer movement (cashiers, store managers, butchers, delivery drivers, security guards). Inclusion of staff devices severely inflates customer dwell-time metrics (e.g., 9-hour employee shifts registered as shopping visits). 

The pipeline applies a strict spatial filter ($\text{accuracy\_m} \le 30$) and classifies roles dynamically using spatial zones (`HEAD_OFFICE`, `CASH_REGISTERS`, `BUTCHERY`) and temporal arrival windows (5:00–6:00 AM delivery windows).

```sql
WITH clean_geo AS (
  -- Filter out GPS noise (>30m accuracy radius)
  SELECT * 
  FROM `bqproj-435911.Final_Project_2025.geolocation`
  WHERE accuracy_m <= 30
),
device_stats AS (
  SELECT 
    device_id,
    -- Count pings in designated store operational zones
    COUNTIF(area = 'HEAD_OFFICE') AS office_pings,
    COUNTIF(area = 'CASH_REGISTERS') AS register_pings,
    COUNTIF(area = 'BUTCHERY') AS butchery_pings,
    COUNTIF(area = 'SUPERMARKET') AS shopping_pings,
    -- Detect early morning supply chain deliveries (Mon / Thu 5-8 AM)
    COUNTIF(
      EXTRACT(HOUR FROM timestamp) BETWEEN 5 AND 8 
      AND EXTRACT(DAYOFWEEK FROM timestamp) IN (2, 5)
    ) AS delivery_time_pings,
    COUNT(*) AS total_pings
  FROM clean_geo
  GROUP BY device_id
)
SELECT 
  device_id,
  CASE 
    WHEN office_pings > register_pings AND office_pings > 100 THEN 'manager'
    WHEN register_pings > shopping_pings AND register_pings > 100 THEN 'cashier'
    WHEN butchery_pings > 10 THEN 'butcher'
    WHEN delivery_time_pings > 0 AND total_pings < 50 THEN 'delivery_guy'
    WHEN total_pings > 200 THEN 'general_staff'
    ELSE 'customer'
  END AS calculated_role
FROM device_stats;
```

#### Ground Truth Classification Validation
* **Stationary Staff (100% Accuracy):** Perfectly identified 1 Store Manager, 15 Cashiers, and 4 Butchers against official HR logs.
* **Mobile Staff & Security Findings:** Discovered that Security Guards (`security_guy`) exhibited an 88.65% GPS error rate due to metallic guard-booth shielding and perimeter patrols. Security telemetry was subsequently excluded from spatial traffic heatmaps to eliminate distortion.

---

### 2. Customer Loyalty Segmentation & VIP Analytics

Shoppers are categorized into four distinct behavioral tiers to analyze revenue concentration and retention:
1. **Repeat (App Users):** 621 devices (Core revenue engine, 87% of digital sales).
2. **One-Time (App Users):** 140 devices.
3. **Not Paying (Store Visitors):** 299 devices (browsed without purchasing).
4. **Walk-in (No App / No Phone):** 3,596 transactions recorded in POS without matching geolocation ID.

```sql
WITH valid_roles AS (
  SELECT DISTINCT device_id, role
  FROM `bqproj-435911.Final_Project_2025.geolocation`
  WHERE role IN ('repeat_customer', 'one_time_customer', 'not_paying')
),
sales_data AS (
  SELECT 
    customer_id,
    COUNT(DISTINCT sale_id) AS total_transactions,
    SUM(total) AS total_spend
  FROM `bqproj-435911.Final_Project_2025.log_sales`
  GROUP BY 1
),
visit_data AS (
  SELECT 
    device_id,
    COUNT(DISTINCT DATE(timestamp)) AS visit_days
  FROM `bqproj-435911.Final_Project_2025.geolocation`
  WHERE accuracy_m <= 30
  GROUP BY 1
)
SELECT * FROM (
  SELECT 
    'Most Frequent (Visits)' AS category,
    v.device_id,
    v.visit_days AS value,
    r.role
  FROM visit_data v
  JOIN valid_roles r ON v.device_id = r.device_id
  ORDER BY value DESC LIMIT 5
)
UNION ALL
SELECT * FROM (
  SELECT 
    'Top Spender (Shekels)' AS category,
    s.customer_id,
    ROUND(s.total_spend, 2) AS value,
    r.role
  FROM sales_data s
  JOIN valid_roles r ON s.customer_id = r.device_id
  ORDER BY value DESC LIMIT 5
);
```

#### VIP Customer Highlight
Cross-referencing frequency and monetary metrics revealed anchor VIP customers:
* **`rep_207`**: 51,527 NIS cumulative spend (98 unique visit days).
* **`rep_182`**: 51,237 NIS cumulative spend (100 unique visit days).
* **Recommendation:** Automated issuance of 100 NIS in-app reward vouchers and personalized appreciation messaging to retain anchor spenders.

---

## Statistical & Econometric Modeling

### Model 1: Dwell Time vs. Average Spend (OLS Regression)

To test the hypothesis that increasing store duration directly expands basket size, an Ordinary Least Squares (OLS) model was fitted on clean customer transactions ($0 < \text{dwell\_minutes} \le 180$).

$$\text{Basket Total (NIS)} = \beta_0 + \beta_1 \cdot \text{DwellMinutes} + \varepsilon$$

```
Linear Regression Summary: Spend vs. Dwell Time
--------------------------------------------------
Slope (β1)        : +7.62 NIS / minute
Intercept (β0)    : 25.65 NIS
R-Squared (R²)    : 0.7593 (75.9% variance explained)
P-Value           : < 0.0001 (Statistically Significant)
Sample Size (N)   : Clean Transaction Logs
```

#### Spend Growth by Duration Tier
* **0–10 Minutes (Quick errand):** Average spend = **74.7 NIS**
* **21–30 Minutes:** Average spend = **220.4 NIS**
* **41–60 Minutes:** Average spend = **414.1 NIS**
* **60+ Minutes (Full basket):** Average spend = **669.1 NIS**

**Business Takeaway:** Extending average customer stay by just **5 minutes** yields an estimated **+38.10 NIS increase per transaction**. In-store investments in air conditioning, background music, optimized aisle pacing, and tasting stations carry direct, measurable ROI.

---

### Model 2: Weekly Traffic Trend & Churn Analysis

$$\text{Weekly Unique Visitors} = \alpha_0 + \alpha_1 \cdot \text{WeekNumber} + \varepsilon$$

```
Linear Regression Summary: Weekly Traffic Trend
--------------------------------------------------
Slope (α1)        : -12.89 visitors / week
Intercept (α0)    : 1599.04 visitors
R-Squared (R²)    : 0.2036
P-Value           : 0.018 (Statistically Significant at α = 0.05)
```

**Business Takeaway:** The store exhibits a steady attrition rate of ~13 shoppers per week. This downward trajectory highlights market saturation at the current location and underscores the urgency of geographical expansion.

---

## Operational Efficiency & Supply Chain Optimization

### 1. Store Traffic Heatmap & Shift Scheduling

Analysis of hour-by-day visitor counts revealed severe operational concentration:
* **Peak Demand ("The Thursday Effect"):** Thursday evenings (17:00–20:00) and Friday mornings (8:00–12:00) represent peak foot traffic. Average basket sizes jump **+55%** from 380 NIS (Sun–Wed) to 590 NIS (Thu–Fri).
* **Daily Peak Hour:** 14:00 records peak revenue across all days (~3.1M NIS cumulative across dataset, 6k transactions).
* **Operations Gap at 16:00:** Data revealed a sharp, unnatural dip in transactions at 16:00 across all weekdays, indicating shift handoff friction or register closures.

```
Operating Hours Heatmap Snippet (Hourly Visitor Counts)
Day         08:00  09:00  10:00  12:00  14:00  16:00  18:00  20:00
------------------------------------------------------------------
Sunday       ---    480    421    417    409    73*   425    79
Monday       ---    442    386    396    370    79*   402    78
Thursday     ---    ---    534    577    338   553    554   337
Friday       504    590    485    590    483   ---    ---   ---
(*Note: Operational dip at 16:00 on weekdays)
```

### 2. Supply Chain Anomaly Tracking

Cross-referencing delivery driver arrivals (`role = 'delivery_guy'`, early morning pings) against expected schedules identified two distinct supply chain failure modes:
1. **Scheduled Holiday Closures (Zero Pings):** Shavuot (June 2, 2025) and Yom Kippur (October 2, 2025).
2. **Unexplained Supply Disruptions (Store Open, Zero Deliveries):**
   * **Thursday, June 26, 2025**
   * **Thursday, August 7, 2025**
   * **Thursday, November 27, 2025**
   * *Impact:* Out-of-stock events during peak Thursday shopping windows.

---

## Strategic Expansion & Spatial Intelligence

To guide network expansion, sales data was integrated with national demographic records (`Lamas_israel_2025`) and competitor store locations (`supermarkets_il`).

### Opportunity Score Formula

$$\text{Opportunity Score} = \frac{\text{City Total Population}}{\max(\text{Existing Competitor Supermarkets}, 1)}$$

```sql
SELECT 
  TRIM(t1.city_name) AS City,
  t1.total_population AS Population,
  COUNT(t2.id) AS Competitor_Count,
  CAST(t1.total_population / GREATEST(COUNT(t2.id), 1) AS INT64) AS Opportunity_Score
FROM `bqproj-435911.Final_Project_2025.Lamas_israel_2025` AS t1
LEFT JOIN `bqproj-435911.Final_Project_2025.supermarkets_il` AS t2
  ON TRIM(t1.city_name) = TRIM(t2.city)
GROUP BY 1, 2
HAVING t1.total_population > 15000
ORDER BY Opportunity_Score DESC
LIMIT 15;
```

### Expansion Rankings Table

| Rank | City | Population | Competitor Stores | Opportunity Score | Strategic Assessment |
| :---: | :--- | :---: | :---: | :---: | :--- |
| **1** | **Modi'in Illit** | **91,000** | **0** | **91,800** | **Top Priority Target (High density, zero major chain rivals)** |
| 2 | Ramla | 88,000 | 1 | 88,800 | High potential, minimal presence |
| 3 | Beitar Illit | 74,000 | 0 | 74,300 | Strong religious demographic alignment |
| 4 | Kiryat Ata | 67,000 | 1 | 67,400 | Northern hub expansion |
| 5 | Umm al-Fahm | 61,000 | 0 | 61,900 | High population, underserved market |
| -- | *Tel Aviv* | 460,000+ | 243 | Low | Highly Saturated (Severe cannibalization risk) |
| -- | *Jerusalem* | 950,000+ | 198 | Low | Highly Saturated |

**Strategic Fit:** Modi'in Illit perfectly aligns with the chain's operational strengths. The demographic profile features large households preparing extensively for the Sabbath, matching the store's high-volume Thursday/Friday sales pattern.

---

## BI Dashboard Architecture (Looker Studio)

The master BI dataset compiles cleaned sales, spatial metadata, and customer tiers into a single flat table for high-performance Looker Studio reporting:

```sql
WITH user_roles AS (
  SELECT DISTINCT device_id, role
  FROM `bqproj-435911.Final_Project_2025.geolocation`
)
SELECT 
  s.timestamp,
  EXTRACT(DATE FROM s.timestamp) AS date,
  EXTRACT(HOUR FROM s.timestamp) AS hour,
  CASE EXTRACT(DAYOFWEEK FROM s.timestamp)
    WHEN 1 THEN '1. Sunday'
    WHEN 2 THEN '2. Monday'
    WHEN 3 THEN '3. Tuesday'
    WHEN 4 THEN '4. Wednesday'
    WHEN 5 THEN '5. Thursday'
    WHEN 6 THEN '6. Friday'
    WHEN 7 THEN '7. Saturday'
  END AS day_name,
  s.total AS revenue,
  CASE 
    WHEN u.role = 'repeat_customer' THEN 'Repeat (App)'
    WHEN u.role = 'one_time_customer' THEN 'One-Time (App)'
    ELSE 'Walk-in (No App)'
  END AS customer_type,
  s.dwell_minutes
FROM `bqproj-435911.Final_Project_2025.log_sales` s
LEFT JOIN user_roles u ON s.customer_id = u.device_id
WHERE (s.dwell_minutes <= 180 OR s.dwell_minutes IS NULL);
```

### Dashboard View Structure

```
+--------------------------------------------------------------------+
| LOOKER STUDIO ENTERPRISE DASHBOARD                                 |
+--------------------------------------------------------------------+
| [PAGE 1: EXECUTIVE OVERVIEW (C-Suite)]                             |
|  - Macro KPIs: Revenue (16.6M NIS), Visits (34.6K), Avg Basket     |
|  - Revenue Trend Line (Daily Seasonality & Holiday Drops)          |
|  - Digital Loyalty Mix Pie Chart (87% Repeat App / 10.4% Walk-in)   |
+--------------------------------------------------------------------+
| [PAGE 2: OPERATIONS & EFFICIENCY (Store Management)]              |
|  - Staffing Heatmap: Traffic Volume by Day & Hour                  |
|  - Customer Dwell Time Distribution (Polynomial Trend Curve)       |
|  - Average Basket Size by Day of Week (Thursday Surge +55%)        |
+--------------------------------------------------------------------+
| [PAGE 3: STRATEGY & EXPANSION (VP Business Development)]           |
|  - Dwell Time vs. Spend Scatter Plot with Trendline                |
|  - Expansion Target Score Rankings (Modi'in Illit #1)              |
+--------------------------------------------------------------------+
```

---

## Repository Structure

```
├── sql/
│   └── supermarket_analytics_master.sql   # Full 5-part BigQuery SQL script
├── data_dictionary/
│   └── schema_overview.md                 # Entity-relationship & field docs
├── visual_assets/
│   ├── executive_overview_dashboard.png   # Looker Studio Page 1 screenshot
│   ├── operations_heatmap_dashboard.png   # Looker Studio Page 2 screenshot
│   ├── strategy_expansion_dashboard.png   # Looker Studio Page 3 screenshot
│   └── spend_vs_dwell_regression.png      # OLS Regression Scatter Plot
├── docs/
│   ├── presentation_deck.pdf              # Final capstone slides
│   └── executive_summary_report.pdf       # Comprehensive project report
└── README.md                              # Main portfolio documentation
```

---

## Author & Project Credits

* **Author:** Tim Brodsky
* **Program:** Data Analyst Program (Google & Reichman Tech School)
* **Capstone Grade:** 99 / 100
