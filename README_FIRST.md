# Week 4 — Spark, PySpark and Delta Lake

Start with `Week4_Spark_Delta_Candidate_Labs.docx`. All required input files and starter code are included.

## Run locally
1. Install Java 17 and Python 3.11+.
2. Create a virtual environment and run `pip install -r requirements.txt`.
3. Run `python verify_setup.py`.
4. Run a starter with `spark-submit starter/day1_dataframe_foundations.py`.

## Run in Microsoft Fabric
Upload the relevant CSV folder to Lakehouse **Files**, attach the Lakehouse to a notebook, and adapt only the input/output root paths. Fabric Runtime 2.0 uses Spark 4.1 and Delta 4.2; the lab API calls are deliberately portable.

## Working rule
Do not edit `provided_data`. Write every result under a separate `work/` or Lakehouse path. Retain screenshots or exported output, code, and the completed evidence log.
