# AetherMart: E-Commerce Data Infrastructure

## 🚀 Project Overview

AetherMart is a comprehensive project designed to simulate the backend transformation of a high-growth e-commerce platform serving customers across web, mobile, and retail channels.

The goal was to architect a solution that evolves from a fragile, single-node database into a globally scalable, high-availability hybrid ecosystem. The final architecture integrates ACID-compliant relational clusters for transactional data (products, customers, orders, reviews), NoSQL document stores for flexible catalogs and unstructured data, and vector-based AI search engines—all governed by a custom Python orchestration engine with comprehensive data governance.

## 🏗️ Technical Architecture

This project tackles real-world enterprise challenges:

- **High Availability:** Zero-downtime architecture using synchronous Multi-Master clustering with automatic failover.
- **Polyglot Persistence:** Hybrid SQL (MariaDB) and NoSQL (MongoDB) data flow supporting diverse data types.
- **AI-Driven Search:** Semantic search and recommendations using LLM vector embeddings (Google Gemini API).
- **Automated Governance:** Custom ETL orchestration with DAG dependency management, centralized auditing, and data lineage tracking.
- **Multi-Channel Support:** Unified data infrastructure serving web, mobile app, and retail partnership channels.

## 🛠️ Technology Stack

| Domain | Technologies Used |
|--------|------------------|
| **Cloud & Infrastructure** | AWS EC2 (Ubuntu Linux), AWS Security Groups, VPC |
| **Databases** | MariaDB (InnoDB, Galera Cluster), MongoDB (NoSQL) |
| **Orchestration & ETL** | Python (Custom DAG Engine), Bash Scripting, SQL (Advanced) |
| **AI & Machine Learning** | Google Gemini API (Text Embeddings), Cosine Similarity Search |
| **Security & Compliance** | STRIDE Threat Modeling, RBAC, TLS Encryption, PII Masking |

## 📽️ Engineering Phases & Implementation Demos

This project was executed in six distinct engineering phases, evolving from foundational setup to advanced orchestration.

### Phase 1: Infrastructure Initialization & ELT Design

**Focus:** Core Infrastructure, Schema Design, Data Ingestion.

- Deployed secure database instances on AWS EC2 with proper network configuration.
- Designed a normalized 3NF schema optimized for multi-domain e-commerce logic (products, customers, orders, reviews).
- Built a robust ELT pipeline using `LOAD DATA LOCAL INFILE` for high-throughput ingestion.
- Implemented advanced SQL features: Views, Virtual Columns, Stored Procedures, and User-Defined Functions.
- Established role-based access control (RBAC) and initial data cleansing protocols.

📺 **Watch Demo:** [Infrastructure Setup & Advanced SQL](https://youtu.be/3N_lwt2Y338)

### Phase 2: Performance Engineering & Scalability

**Focus:** Horizontal Scaling, Database Triggers, Partitioning.

- Implemented Range Partitioning on the Orders table to optimize historical queries and manage growing transaction volumes.
- Developed complex SQL triggers for automated audit logging, data integrity enforcement, and business rule validation.
- Analyzed CAP theorem trade-offs to architect an optimal partitioning strategy for multi-channel data growth.
- Explored NoSQL concepts and evaluated MariaDB ColumnStore for analytical workloads.

📺 **Watch Demo:** [Partitioning Strategies & Triggers](https://youtu.be/5GirB-D2wvU)

### Phase 3: High Availability Clustering (Zero-Downtime)

**Focus:** Disaster Recovery, Replication, Failover.

- Deployed a 3-node MariaDB Galera Cluster (Synchronous Multi-Master).
- Configured Asynchronous Replication for read-scalability.
- **Stress Testing:** Simulated node crashes to verify automatic quorum recovery and zero data loss (RPO=0).

📺 **Watch Demo:** [Galera Clustering & Failover Tests](https://youtu.be/uHO7BUr32a8)

### Phase 4: AI Integration & Vector Optimization

**Focus:** Semantic Search, Query Optimization, LLM Integration.

- Integrated Google Gemini API to generate 768-dimensional vector embeddings for product catalog and customer preferences.
- Implemented Vector Search using Cosine Similarity to enable natural language queries and personalized recommendations.
- Optimized query performance by ~80% using Composite Indexing, query analysis with `EXPLAIN`, and index optimization.
- Established refined data preparation pipelines for AI model consumption.

📺 **Watch Demo:** [Vector Embeddings & Optimization](https://youtu.be/93OSozRPtcQ)

### Phase 5: Hybrid Architecture (SQL + NoSQL)

**Focus:** Polyglot Persistence, Migration Strategies.

- Integrated MongoDB to handle unstructured data (clickstreams, logs, social media interactions) and flexible product schemas.
- Developed dynamic Python migration scripts to transfer relational data to document stores while maintaining data integrity.
- Designed a comprehensive Data Lake vs. Data Warehouse strategy for multi-channel data ingestion and analytics.
- Established security best practices for heterogeneous data environments, including PII protection across platforms.

📺 **Watch Demo:** [Hybrid Architecture & Migration](https://youtu.be/6ye3B8RtrCs)

### Phase 6: Orchestration, Security & Governance

**Focus:** Automation, Security Hardening, Final Synthesis.

- **Custom Orchestrator:** Built a Python-based ETL engine with DAG dependency logic, exponential backoff retries, centralized logging, and proactive monitoring.
- **Data Governance:** Established comprehensive data quality standards, metadata management protocols, and data lineage tracking across all systems.
- **Security:** Conducted a STRIDE threat assessment; hardened network access via Bastion Hosts, implemented TLS encryption, and established PII masking protocols.
- **Compliance:** Designed audit trails and governance frameworks to meet regulatory requirements for multi-channel customer data.

📺 **Watch Demo:** [ETL Orchestration & Final Architecture](https://youtu.be/1-bMMFo0XKQ)

## 📂 Repository Structure

```
├── Phase1&2_Foundation_Scaling/      # Schema SQL, Generator Scripts, Initial ELT, Partitioning Logic, Audit Triggers
├── Phase3_HA_Cluster/      # Galera Configs, Replication Setup
├── Phase4_AI_Search/       # Vector Generation (Python), Indexing SQL
├── Phase5_NoSQL_Hybrid/    # MongoDB Migration Scripts
├── Phase6_Orchestration/   # Custom Python ETL Engine, Security Docs
```

## 🧠 AI Augmented Development

This project leveraged Google Gemini & Anthropic Claude AI as a "human-in-the-loop" coding assistant to:

- Accelerate boilerplate code generation for schema definitions.
- Debug complex configuration files for distributed clustering.
- Generate synthetic metadata for vector search implementation.

**Note:** All architectural decisions, logic implementation, and security configurations were designed and validated by the lead engineer.