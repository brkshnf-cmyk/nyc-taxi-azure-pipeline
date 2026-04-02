# \# 🚕 NYC Yellow Taxi — Azure Data Engineering Pipeline

# 

# \## Project Overview

# End-to-end Azure data engineering pipeline ingesting NYC TLC Yellow 

# Taxi trip records into a medallion architecture (Bronze → Silver → Gold),

# modelled as a star schema and visualised in Power BI.

# 

# Built as a portfolio project demonstrating production-grade data 

# engineering practices on Microsoft Azure.

# 

# \## 🏗️ Architecture

# ```

# NYC TLC CDN → ADF (HTTP) → ADLS Gen2 Bronze

# → Synapse Analytics SQL → ADLS Gen2 Silver/Gold

# → Power BI Dashboard

# ```

# 

# \## 🛠️ Tech Stack

# | Tool | Purpose |

# |------|---------|

# | Azure Data Lake Storage Gen2 | Medallion storage — Bronze/Silver/Gold |

# | Azure Data Factory | Orchestration \& automated ingestion |

# | Azure Synapse Analytics | SQL transformation \& modelling |

# | Azure SQL Database | Pipeline control/logging table |

# | Power BI | Dashboard \& reporting |

# | GitHub | Version control |

# 

# \## 📁 Repository Structure

# ```

# nyc-taxi-azure-pipeline/

# ├── pipelines/

# │   ├── pl\_ingest\_yellow\_taxi\_manual.json

# │   └── triggers/

# │       └── tr\_tumbling\_yellow\_taxi\_daily.json

# ├── linked\_services/

# │   ├── ls\_adls\_nyctaxi.json

# │   └── ls\_http\_tlc.json

# ├── datasets/

# │   ├── ds\_http\_yellow\_taxi\_source.json

# │   └── ds\_sink\_yellow\_taxi\_bronze.json

# ├── sql/               ← Synapse SQL scripts (Phase 2)

# └── docs/              ← Architecture diagrams (Phase 5)

# ```

# 

# \## 📦 Data

# \- \*\*Source:\*\* NYC Taxi \& Limousine Commission (TLC)

# \- \*\*Dataset:\*\* Yellow Taxi Trip Records

# \- \*\*Format:\*\* Parquet

# \- \*\*Coverage:\*\* 2024–2026

# \- \*\*URL:\*\* https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

# 

# \## 🔄 Pipeline: pl\_ingest\_yellow\_taxi\_manual

# \- Pulls monthly Parquet files directly from NYC TLC CDN via HTTP

# \- Parameterised by year / month / day

# \- Triggered daily at 06:00 UTC via tumbling window trigger

# \- Binary copy — raw files landed unmodified in bronze layer

# \- Retry policy: 3 retries, 60 second interval

# 

# \## 🥉 Bronze Layer Structure

# ```

# bronze/yellow-taxi/year=YYYY/month=MM/day=DD/

# ```

# 

# \## ✅ Project Progress

# \- \[x] Phase 1 — Ingestion pipeline (complete)

# \- \[ ] Phase 2 — SQL modelling \& star schema

# \- \[ ] Phase 3 — Power BI dashboard  

# \- \[ ] Phase 4 — Testing \& monitoring

# \- \[ ] Phase 5 — Portfolio packaging

# 

# \## 🚀 How to Run

# 1\. Clone this repo

# 2\. Deploy ADF using JSON files in `/pipelines` and `/linked\_services`

# 3\. Configure linked service credentials for your own ADLS account

# 4\. Publish and activate the tumbling window trigger

