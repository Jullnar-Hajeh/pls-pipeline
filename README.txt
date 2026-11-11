PLS Trainee Task - Public Libraries Mini-Pipeline (Airflow + Spark)
====================================================================

Start with TASK.pdf. It has the goal, requirements, acceptance criteria and the 5-day plan.

Contents
--------
TASK.pdf                              The task brief
docs/README_FY2021_PLS_PUD.txt        IMLS notes for FY2021 (read before coding)
docs/README_FY2022_PLS_PUD.txt        IMLS notes for FY2022 (read before coding)

data/fy2020/PLS_FY20_AE_pud20i.csv    Core input: library systems, FY2020  (9,245 rows)
data/fy2021/PLS_FY21_AE_pud21i.csv    Core input: library systems, FY2021  (9,215 rows)
data/fy2022/PLS_FY22_AE_pud22i.csv    Core input: library systems, FY2022  (9,248 rows)

data/stretch/                         Only for the stretch goals
  PLS_FY20_State_pud20i.csv           State totals FY2020, to cross-check your Gold table
  PLS_FY20_Outlet_pud20i.csv          Branch/outlet files, to add branch counts per state
  pls_fy21_outlet_pud21i.csv
  pls_fy22_outlet_pud22i.csv

Notes
-----
- Do not edit the CSV files. Your pipeline must handle them as they are.
- The files do not all use the same text encoding. Check each one before you read it.
- There is no FY2020 README; the FY2021 notes about special codes apply to FY2020 too.
- Data source: IMLS Public Libraries Survey public-use files (imputed versions).
