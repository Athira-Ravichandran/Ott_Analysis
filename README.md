#  OTT Platform Analysis

##  Project Overview

This project analyzes OTT (Over-the-Top) streaming platforms to uncover insights about **content quality, genre trends, platform performance, and audience preferences**.

Using a combination of **Python, MySQL, and Power BI**, the project transforms raw scraped and Kaggle datasets into an interactive, business-ready dashboard.

---

##  Objectives

* Analyze content trends across OTT platforms
* Compare platforms based on quality, freshness, and diversity
* Identify high-performing genres and rating patterns
* Build an end-to-end data pipeline and dashboard

---

##  Tech Stack

* **Python** (Pandas, NumPy) → Data cleaning & transformation
* **MySQL** → Data storage & analytical queries (views, window functions)
* **Power BI** → Interactive dashboards & storytelling
* **Web Scraping + Kaggle Dataset** → Data sources

---

## Data Sources

* Scraped IMDb Top 50 Movies & TV Shows
* Kaggle Netflix Dataset (movies & TV shows metadata)

---

##  Data Pipeline

1. Web scraping to collect IMDb data
2. Kaggle dataset ingestion
3. Data cleaning & transformation using Python
4. Feature engineering:

   * Decade, Era classification
   * Vote-weighted rating
   * Genre mapping
   * Recent content flag
5. Data loaded into MySQL
6. SQL Views created for analysis
7. Power BI dashboards built on top of views

---

##  Key Features

###  Python-Based Data Processing

* Cleaned and merged multiple datasets
* Standardized genres using mapping logic
* Created derived features like:

  * `vote_weighted_score`
  * `is_recent`
  * `popularity_tier`

---

###  SQL Analytics (Advanced)

* Created analytical views using:

  * `RANK()`
  * `PERCENT_RANK()`
  * `LAG()`
* Built:

  * `vw_platform_kpis`
  * `vw_titles_enriched`
  * `vw_genre_performance`

---

###  Power BI Dashboards (5 Pages)

####  1. Content Overview

* Total titles, average rating, recent content %
* Trend of content over years
* Content distribution (Movies vs TV Shows)

![Content overview](https://github.com/Athira-Ravichandran/Ott_Analysis/blob/main/images/content%20overview.png)
---

####  2. Genre Analysis

* Average rating by genre
* Genre distribution across content types
* Interactive slicer for deep filtering

![Genre Analysis](https://github.com/Athira-Ravichandran/Ott_Analysis/blob/main/images/genre%20analysis.png)
---
####  3. Platform Analysis

* Scatter plot: Platform value comparison
* Platform-wise average rating
* Content distribution across platforms

![Rating Analysis](https://github.com/Athira-Ravichandran/Ott_Analysis/blob/main/images/rating%20analysis.png)
---

####  4. Rating & Performance Insights

* Rating distribution
* Trend of ratings over time
* Ranking of titles using SQL window functions

---

####  5. Business Insights & Recommendations

* Executive summary
* Key findings
* Data-driven recommendations
![summary](https://github.com/Athira-Ravichandran/Ott_Analysis/blob/main/images/summary.png)
---

## Key Insights

* Drama and Crime genres consistently produce high-rated content
* Netflix leads in volume, while Amazon Prime shows stronger weighted quality
* Older titles tend to have higher ratings due to survivorship bias
* Genre diversity varies significantly across platforms

---


## Exploratory Data Analysis (Python)

Initial analysis and visualizations were created using Python (Matplotlib, Seaborn) before building the Power BI dashboards.

(See `/charts` folder for EDA visuals)


---

## Limitations

* Dataset is partially Netflix-heavy due to available sources
* Platform distribution may not fully represent real-world catalog sizes

---


