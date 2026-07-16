Since you are in "grind mode," I have organized these by the architectural pillars of your project. These are designed to be "brutal" interview questions-the kind that force you to defend every single choice you made.
Don't just answer them; write down a 2-3 sentence explanation for each. If you get stuck on one, that’s your study area for the day.
Part 1: Architecture, Business, & The Medallion Pattern
Why is the "Medallion Architecture" superior to just dumping everything into a single Postgres database?
Explain the concept of "Data Temperature" and how it maps to your Bronze, Silver, and Gold layers.
Why is the Bronze layer immutable? What happens if you need to "fix" raw data?
What is the specific business risk of letting analysts query the Bronze layer directly?
How does the Gold layer differ from a Data Mart?
Why did we choose a Retail domain for this project? Could you justify the choice to an interviewer who wants to know why not Finance or Healthcare?
What is "Schema Drift," and at which layer of your architecture is it easiest to handle?
In a Medallion architecture, where does the "System of Record" live?
Explain the difference between "Data Engineering" and "Data Analysis" in the context of your Retail Lakehouse.
If the business asks for a "live" report in 5 seconds, which layer should they query and why?
Part 2: Object Storage (MinIO) & Infrastructure (Docker)
Why use MinIO/S3 instead of a traditional file system (like your Arch Linux root directory) for data storage?
What does "S3-compatible API" actually mean for your code portability?
Explain the concept of "Object Storage Semantics"-why are there no "real" folders?
Why did you choose Docker over installing the entire stack natively on Arch Linux?
What is the danger of running multiple services (Spark, Airflow, MinIO) in one Docker container vs. the docker-compose approach?
How does Docker networking (bridge) allow your Airflow container to communicate with the Spark master?
What are the pros and cons of using named volumes vs. bind mounts in your setup?
If your Docker host runs out of disk space, how does that affect an immutable storage system like MinIO?
How do you handle secrets (access keys/passwords) in your Docker setup?
Explain the concept of a healthcheck in docker-compose-why is it critical for dependencies like MinIO?
Part 3: Orchestration (Apache Airflow)
What is a DAG, and why is it "Directed" and "Acyclic"?
What happens to the state of a task if the Airflow worker crashes mid-execution?
Why use Airflow instead of a simple cron job or a shell script loop?
Explain the difference between execution_date and data_interval_end.
How do you ensure "Idempotency" in your Airflow DAGs? (i.e., can you rerun a job for last Tuesday without duplicating data?)
What is the purpose of the XCom mechanism in Airflow, and when is it a bad idea to use it?
Why should you never perform heavy data processing directly inside an Airflow worker?
How does backfilling work in Airflow, and why is it useful for your Retail project?
Part 4: The Compute Engine (Apache Spark)
Explain the relationship between the Spark Driver and Spark Executors.
What is "Lazy Evaluation," and how does it optimize your pipeline?
What is an RDD, and why do we prefer DataFrames for your retail pipeline?
Why does Spark perform better on massive datasets than a standard Python Pandas script?
Explain the difference between Transformation and Action in Spark.
What is a "Shuffle" in Spark, and why is it the most expensive operation in your pipeline?
What is "Partitioning" in Spark, and how do you choose the right number of partitions?
Why is the "Driver" process sometimes a bottleneck in a small cluster?
What happens if your Spark job runs out of memory on the executors?
Explain Broadcast Variables-when would you use them in your Retail project?
Why do we prefer Parquet (columnar) over CSV (row-based) for Spark processing?
What is the purpose of the SparkSession?
Part 5: Delta Lake (ACID & Storage)
What is the _delta_log folder, and why does it make Delta Lake "ACID compliant"?
How does Delta Lake perform an "Upsert" (MERGE) compared to overwriting a file?
What is "Time Travel" in Delta Lake, and how would it help you fix a corrupted pipeline run?
Explain "Schema Enforcement"-how does it prevent bad data from hitting your Silver layer?
Why does Delta Lake improve "Small File Problem" (explain file compaction/OPTIMIZE)?
How does Delta Lake's transaction log handle multiple writers trying to update the same table?
Is Delta Lake a database or a file format? Defend your answer.
What is the difference between a Delta MERGE and an INSERT OVERWRITE?
Why is the metadata store (Delta log) separate from the data files (Parquet)?
If you accidentally delete a Parquet file from the folder, does the Delta table still work? Why or why not?
Part 6: Transformation (dbt) & Data Quality (Great Expectations)
Why use dbt to transform data instead of just running PySpark jobs for everything?
Explain the benefit of "Modular" SQL models in dbt.
How does dbt handle "Incremental" materialization, and why is it efficient?
What is the difference between dbt ref() and hard-coding table names in SQL?
Why is a test that checks for null values in order_id essential in your Retail project?
What is the benefit of defining data quality rules outside the transformation logic (the Great Expectations philosophy)?
How do you handle a "soft failure" vs. a "hard failure" in a data pipeline (e.g., if one row is bad, does the whole job fail)?
Explain the "Lineage" visualization in dbt-why does a hiring manager care about it?
If your silver to gold transformation fails due to a schema change, how does dbt help you debug?
What is an SCD Type 2 transformation, and why is it needed for dim_customer?
How to use these: Don't write essays. Stand up, pace around, and explain the answer to an imaginary interviewer. If you can't explain it clearly in under 60 seconds, you don't know the topic well enough yet.

This list is designed to push you past the "I have a streaming pipeline" phase and into the "I know how streaming systems break" phase. In an interview, these are the questions that separate the people who followed a tutorial from the engineers who built a system.
Topic 1: Message Broker Fundamentals (Kafka/Redpanda)
What is the difference between a message queue (like RabbitMQ) and a log-based broker (like Kafka/Redpanda)?
Why does Kafka/Redpanda use "partitions" instead of just having one big stream?
How is ordering guaranteed in Kafka? Is it guaranteed across the entire topic or only within a partition?
What is the "consumer offset," and where is it stored?
What happens to messages in a Kafka topic after they are read? (Retention policy vs. Deletion).
How does Kafka achieve high throughput with sequential disk I/O?
Explain the concept of a "Consumer Group." How does it enable load balancing?
What is a "rebalance" in a consumer group, and why is it expensive?
What happens if a consumer dies? How does the broker know it's dead?
How does Kafka handle broker replication and leader-follower failover?
What is the purpose of the acks configuration (acks=0, 1, all) in the producer?
Why did we choose Redpanda for this project?
How does partitioning affect the scalability of your consumer?
What is a "compacted topic," and would it be useful for your retail dataset?
How does Kafka manage high availability?
Topic 2: Spark Structured Streaming (Processing)
Explain the difference between Micro-batch processing and Continuous processing.
Why is the "Checkpoint Location" non-negotiable for production streaming?
What happens to your state if you accidentally delete the checkpoint directory?
How does Spark manage the "State Store" for windowed aggregations?
What is "Watermarking," and how does it prevent memory overflows?
What happens to a record that arrives after the watermark has passed?
How does Spark handle late-arriving data (e.g., an order generated at 10:00 but arriving at 10:30)?
Explain "Event Time" vs. "Processing Time." Why does the difference matter for your retail dashboard?
How does trigger configuration (e.g., once, availableNow, interval) change the pipeline behavior?
Can you join a streaming DataFrame with a static DataFrame? How?
How does Spark handle backpressure if the streaming rate exceeds processing capacity?
Why should you avoid foreachBatch unless absolutely necessary?
What is the "exactly-once" mechanism in Spark Structured Streaming?
How does Delta Lake’s transaction log contribute to exactly-once processing?
What happens if the Spark driver crashes while the executors are running?
Can you have multiple streaming queries reading from the same source?
What is an "Output Sink"?
How does Spark handle schema evolution in a streaming context?
Explain the difference between "Complete," "Append," and "Update" output modes.
How do you perform a "stateful" aggregation (e.g., tracking a user's total daily spend)?
Topic 3: Consistency, Fault Tolerance, & Reliability
What is the "at-least-once" guarantee, and why is it dangerous for financial metrics like "Total Revenue"?
How do you design an idempotent downstream sink (so if the job restarts, you don't double-count orders)?
How does Spark Streaming handle a network partition between Kafka and the worker nodes?
What is "data skew" in a streaming pipeline, and how does it hurt throughput?
Why might large window sizes (e.g., 24-hour windows) be risky for streaming state management?
How do you reconcile your live-streamed revenue total with the authoritative batch total at the end of the day?
If your event generator crashes, how do you restart the stream without re-reading the entire history?
What is the "Stop-the-world" effect in Java garbage collection, and how does it impact streaming latency?
How do you optimize Spark streaming to achieve sub-second latency?
Why is it harder to debug a streaming job compared to a batch job?
How do you instrument your pipeline to alert you when streaming lag exceeds 5 minutes?
Why is serializing data as JSON in Kafka a potential performance bottleneck?
What are the pros/cons of using Avro/Protobuf vs. JSON for streaming events?
How does "grace period" for late data differ from "watermarking"?
What is the "thundering herd" problem when restarting a streaming application with large state?
Topic 4: Architectural Trade-offs
What is the "Lambda Architecture," and why do some engineers hate it?
Compare the Lambda architecture to the "Kappa Architecture."
Why not just stream everything and skip batch processing entirely?
How does "event-time processing" allow you to handle data that arrives out of order?
If you needed to perform an update on a previous event in the stream (e.g., a cancelled order), how would you handle it?
How does your choice of partitioning in Kafka affect Spark's ability to parallelize processing?
In your project, why stream into Gold directly instead of Bronze? What are the risks of doing that?
How do you define a "healthy" stream in your project? (e.g., throughput vs. latency vs. errors).
Why is it harder to maintain a streaming pipeline over 2+ years compared to a batch one?
If the business wants the live dashboard to be 100% accurate (not approximate), does that change your architectural choice?
Your mission: Pick 5 from this list. Write down your logic for each. If you hit a wall (e.g., "I don't actually know how Spark handles state stores"), stop and read the official Spark Structured Streaming documentation on that specific concept. Do not move to the next phase until you can defend your choices on all 60 of these.
Ready to start the grind?
Phase 4 is the "Value" phase. This is where you stop building infrastructure and start building applications. If Phase 1-3 were about the pipes, Phase 4 is about the product.
These questions aren't just about Python or SQL-they are about how you safely deliver model results to non-technical business users (the CRM/BI).
Group 1: Feature Engineering & RFM (10 Questions)
Why is RFM (Recency, Frequency, Monetary) the industry standard for retail churn, and what are its inherent limitations?
How do you handle "cold starts" for new customers who have 0 frequency in your RFM model?
Why is it dangerous to use a "global average" for Monetary value? How should you handle outliers (e.g., a bulk purchase by a B2B client) in your feature engineering?
When calculating Recency, do you use the absolute date of the last order, or the relative date to the time of prediction? Why does this matter?
How would you handle a customer who has high Frequency but hasn't purchased in a long time (high Recency)?
Does your RFM model account for seasonality? If not, how would the model react if all your data is from December (holiday season)?
Explain the "Feature Store" concept. Does your project have a dedicated one, or is the Gold layer acting as one?
How do you deal with missing values in your features? (e.g., if a customer has no monetary data?)
Can RFM be applied to non-retail datasets? Give an example.
If you were to add "Product Category Diversity" as a feature, how would you encode that into a format the model can process?
Group 2: ML Model Lifecycle & Serving (15 Questions)
What is "Training-Serving Skew," and how would it manifest in your model?
How does the scikit-learn version used in training affect the deployment in your FastAPI container?
Why did we choose pickle for model serialization? What are the security and versioning risks?
What is "Model Drift," and how would you monitor it in this pipeline?
If your churn prediction accuracy drops by 10%, how do you determine if it's the data that changed or the model that is outdated?
Explain the trade-off between latency (predicting per-request) and throughput (batch-scoring all customers at night). Which does your project use?
Why wrap the model in a FastAPI app instead of just running the script inside Airflow?
How do you handle concurrent requests in FastAPI if the inference takes 2 seconds?
What is an API Versioning strategy, and how would you update the model without breaking existing CRM integrations?
Define "Precision-Recall Trade-off" in the context of Churn: Is it worse to miss a churner (False Negative) or annoy a loyalist with a retention email (False Positive)?
What does "class weight='balanced'" actually do under the hood in a Random Forest?
How do you serialize your feature engineering steps (e.g., Scaling/Normalization) so they are applied to new data exactly as they were in training?
Why is it bad practice to train your model on the entire dataset without a hold-out test set?
How do you ensure the model you saved is the exact one you tested?
Can you use dbt to validate your model features before the model runs?
Group 3: Reverse ETL & Operational Systems (15 Questions)
What is the fundamental difference between ETL (Extract-Transform-Load) and Reverse ETL?
Why not just write churn scores directly from Spark to the CRM (Postgres) and skip the Reverse ETL script layer?
Explain the "Idempotency" requirement for your Reverse ETL script.
How do you handle a "primary key collision" in your Postgres churn_scores table?
If the Postgres CRM database schema changes, how does your Reverse ETL script react?
How do you handle API rate limits if your CRM were a cloud-based SaaS (like Salesforce) instead of a local Postgres?
What is a "Transactional Outbox Pattern" in Reverse ETL?
How do you ensure the scores in your CRM are "fresh" and not from last month's run?
Explain the difference between a FULL LOAD and an INCREMENTAL LOAD in Reverse ETL.
If the Reverse ETL job fails, how do you prevent the business from acting on stale churn scores?
What is the risk of "Race Conditions" if your batch job and streaming jobs both try to update the CRM?
How do you handle data types in Postgres that don't map cleanly from Pandas (e.g., timestamps)?
Should you delete records in the CRM that no longer exist in your Lakehouse? Why or why not?
How do you log the performance of your Reverse ETL job?
Why is Reverse ETL considered a critical component of "Operational Analytics"?
Group 4: Governance, Lineage, & Discovery (15 Questions)
Why is "Lineage" the most requested feature by data engineers?
If a dashboard in Superset looks wrong, how does OpenMetadata help you find the broken model in dbt?
What is the difference between "Technical Metadata" and "Business Metadata"?
How do you automate the collection of metadata so it's not a manual chore?
What is the "Data Cataloging" problem?
How do you ensure PII (Personally Identifiable Information) isn't accidentally surfaced in your Gold layer?
Explain the concept of a "Data Contract"-how would you enforce one between the source system and your Bronze layer?
What is the role of an "Owner" in OpenMetadata, and why does it matter?
If you rename a column in dbt, what happens to your lineage graph?
Why is it easier to document the system while building it vs. after it's done?
How do you audit who has access to your Silver vs. Gold tables?
Can you automate data quality monitoring based on lineage?
What is "Orphaned Data," and how does a catalog help you find it?
Why do we need a "Glossary" for business terms (e.g., defining exactly what "churned" means) in OpenMetadata?
How do you handle schema evolution tracking in your Delta tables?
Group 5: System Design (The "Big Picture") (10 Questions)
Your churn model is successful. Now the CEO wants "Real-time Churn Scores" for every customer as they browse the website. Does your current architecture support this? Why/Why not?
If the Lakehouse Gold layer becomes massive (Petabytes), will your current dbt workflow scale? What would you change?
How do you handle a "Rollback" of a failed dbt model deployment?
What happens if the source data CSV format changes (e.g., column reordered)? Does the pipeline fail, or does it process bad data?
Is "data accuracy" or "data availability" more important for your churn model?
If you were to add a second retail store, how would your architecture accommodate the new data source?
Explain the trade-offs of using a serverless database (Azure SQL Serverless) vs. a provisioned instance for your CRM.
How do you monitor the "cost" of your infrastructure in Phase 5 (Terraform)?
If you have to choose between a "clean" architecture that is slow to build and a "hacky" architecture that is fast, when is the hacky one acceptable?
What is the single biggest "Single Point of Failure" in your current end-to-end architecture?
How to grind these:
Don't just answer them. When you hit a question like #56 (Real-time churn), draw the architecture on a whiteboard. Where does the ML model move? Where does the data live?
Which group (1–5) do you want to start analyzing? I will act as the interviewer and challenge your answers.

