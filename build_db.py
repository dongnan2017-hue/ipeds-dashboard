"""
Multi-year IPEDS build: loads 2023-24 and 2024-25 into a single DuckDB,
then materialises a METRICS_LONG table for year-over-year trend analysis.

Run this locally before deploying (requires pyodbc + MS Access ODBC driver).
"""

import os, decimal, pyodbc, duckdb, pandas as pd

DB = r"C:\Users\dongn\Videos\IPEDS\dashboard\ipeds.duckdb"

YEARS = [
    {
        "label": "2024-25",
        "accdb": r"C:\Users\dongn\Videos\IPEDS\IPEDS202425.accdb",
        "skip":  {"newVariables24", "tables24", "valueSets24", "varTable24"},
    },
    {
        "label": "2023-24",
        "accdb": r"C:\Users\dongn\Videos\IPEDS\IPEDS202324.accdb",
        "skip":  {"newVariables23", "tables23", "valueSets23", "varTable23",
                  "Tables23"},
    },
]

META_EXCEL = r"C:\Users\dongn\Videos\IPEDS\IPEDS202425Tablesdoc.xlsx"

# ── SQL to build one year's slice of METRICS_LONG ────────────────────────────
# 2024-25 uses DRVCOST2024 for cost; CARNEGIEIC is available in HD2024.
# 2023-24 uses DRVIC2023 for cost;   CARNEGIEIC is NULL (not yet published).
METRICS_SQL = {
    "2024-25": """
        SELECT
            '2024-25'           AS YEAR,
            h.UNITID, h.INSTNM, h.CITY, h.STABBR,
            h.SECTOR, h.ICLEVEL, h.CONTROL,
            h.HBCU, h.TRIBAL, h.MEDICAL, h.LANDGRNT, h.CYACTIVE,
            h.LOCALE, h.INSTSIZE, h.OBEREG,
            h.CARNEGIEIC,
            h.LONGITUD, h.LATITUDE,
            -- enrollment
            e.ENRTOT, e.FTE, e.EFUG, e.EFGRAD, e.ENRFT, e.ENRPT,
            e.PCTENRW,  e.PCTENRWH, e.PCTENRBK, e.PCTENRHS, e.PCTENRAP,
            e.PCTENRAN, e.PCTENRUN, e.PCTENRNR,  e.PCTENR2M,
            e.PCTDEEXC, e.PCTFT1ST,
            ef12.UNDUP AS EF12UNDUP,
            -- admissions
            a.DVADM01, a.DVADM04,
            -- graduation rates
            g.GRRTTOT, g.GRRTM,  g.GRRTW,
            g.GRRTAN,  g.GRRTAP, g.GRRTAS, g.GRRTNH,
            g.GRRTBK,  g.GRRTHS, g.GRRTWH, g.GRRT2M,
            g.GBA4RTT, g.GBA5RTT, g.GBA6RTT,
            g.PGGRRTT, g.TRRTTOT,
            -- cost
            c.CINSON, c.COTSON, c.TUFEYR3,
            -- completions
            d.BASDEG, d.MASDEG, d.ASCDEG, d.DOCDEGRS, d.DOCDEGPP,
            d.CERT1A, d.CERT1B, d.CERT2, d.CERT4,
            -- HR
            r.SALTOTL, r.SFTETOTL, r.SFTEINST,
            -- finance
            f.F1CORREV, f.F1COREXP, f.F2CORREV, f.F2COREXP,
            -- retention / student-faculty ratio
            ef.RET_PCF, ef.RET_PCP, ef.STUFACR,
            -- financial aid
            s.ANYAIDP, s.PGRNT_P, s.PGRNT_A,
            s.AGRNT_P, s.AGRNT_A, s.LOAN_P,
            s.FGRNT_P, s.IGRNT_P, s.SGRNT_P,
            -- outcome measures
            om.OM1TOTLAWDP8, om.OM2TOTLAWDP8,
            om.OM1PELLAWDP8, om.OM1NPELAWDP8,
            -- library
            lib.LEXPTOTF, lib.LEBOOKSP
        FROM HD2024 h
        LEFT JOIN DRVEF2024   e    ON h.UNITID = e.UNITID
        LEFT JOIN DRVEF122024 ef12 ON h.UNITID = ef12.UNITID
        LEFT JOIN DRVADM2024  a    ON h.UNITID = a.UNITID
        LEFT JOIN DRVGR2024   g    ON h.UNITID = g.UNITID
        LEFT JOIN DRVCOST2024 c    ON h.UNITID = c.UNITID
        LEFT JOIN DRVC2024    d    ON h.UNITID = d.UNITID
        LEFT JOIN DRVHR2024   r    ON h.UNITID = r.UNITID
        LEFT JOIN DRVF2024    f    ON h.UNITID = f.UNITID
        LEFT JOIN EF2024D     ef   ON h.UNITID = ef.UNITID
        LEFT JOIN SFA2324     s    ON h.UNITID = s.UNITID
        LEFT JOIN DRVOM2024   om   ON h.UNITID = om.UNITID
        LEFT JOIN DRVAL2024   lib  ON h.UNITID = lib.UNITID
    """,

    "2023-24": """
        SELECT
            '2023-24'           AS YEAR,
            h.UNITID, h.INSTNM, h.CITY, h.STABBR,
            h.SECTOR, h.ICLEVEL, h.CONTROL,
            h.HBCU, h.TRIBAL, h.MEDICAL, h.LANDGRNT, h.CYACTIVE,
            h.LOCALE, h.INSTSIZE, h.OBEREG,
            NULL                AS CARNEGIEIC,
            h.LONGITUD, h.LATITUDE,
            -- enrollment
            e.ENRTOT, e.FTE, e.EFUG, e.EFGRAD, e.ENRFT, e.ENRPT,
            e.PCTENRW,  e.PCTENRWH, e.PCTENRBK, e.PCTENRHS, e.PCTENRAP,
            e.PCTENRAN, e.PCTENRUN, e.PCTENRNR,  e.PCTENR2M,
            e.PCTDEEXC, e.PCTFT1ST,
            ef12.UNDUP AS EF12UNDUP,
            -- admissions
            a.DVADM01, a.DVADM04,
            -- graduation rates
            g.GRRTTOT, g.GRRTM,  g.GRRTW,
            g.GRRTAN,  g.GRRTAP, g.GRRTAS, g.GRRTNH,
            g.GRRTBK,  g.GRRTHS, g.GRRTWH, g.GRRT2M,
            g.GBA4RTT, g.GBA5RTT, g.GBA6RTT,
            g.PGGRRTT, g.TRRTTOT,
            -- cost (from DRVIC2023 — cost was inside IC survey before 2024-25)
            c.CINSON, c.COTSON, c.TUFEYR3,
            -- completions
            d.BASDEG, d.MASDEG, d.ASCDEG, d.DOCDEGRS, d.DOCDEGPP,
            d.CERT1A, d.CERT1B, d.CERT2, d.CERT4,
            -- HR
            r.SALTOTL, r.SFTETOTL, r.SFTEINST,
            -- finance
            f.F1CORREV, f.F1COREXP, f.F2CORREV, f.F2COREXP,
            -- retention / student-faculty ratio
            ef.RET_PCF, ef.RET_PCP, ef.STUFACR,
            -- financial aid (sfa2223_p1 loaded as SFA2223_P1)
            s.ANYAIDP, s.PGRNT_P, s.PGRNT_A,
            s.AGRNT_P, s.AGRNT_A, s.LOAN_P,
            s.FGRNT_P, s.IGRNT_P, s.SGRNT_P,
            -- outcome measures
            om.OM1TOTLAWDP8, om.OM2TOTLAWDP8,
            om.OM1PELLAWDP8, om.OM1NPELAWDP8,
            -- library
            lib.LEXPTOTF, lib.LEBOOKSP
        FROM HD2023 h
        LEFT JOIN DRVEF2023   e    ON h.UNITID = e.UNITID
        LEFT JOIN DRVEF122023 ef12 ON h.UNITID = ef12.UNITID
        LEFT JOIN DRVADM2023  a    ON h.UNITID = a.UNITID
        LEFT JOIN DRVGR2023   g    ON h.UNITID = g.UNITID
        LEFT JOIN DRVIC2023   c    ON h.UNITID = c.UNITID
        LEFT JOIN DRVC2023    d    ON h.UNITID = d.UNITID
        LEFT JOIN DRVHR2023   r    ON h.UNITID = r.UNITID
        LEFT JOIN DRVF2023    f    ON h.UNITID = f.UNITID
        LEFT JOIN EF2023D     ef   ON h.UNITID = ef.UNITID
        LEFT JOIN SFA2223_P1  s    ON h.UNITID = s.UNITID
        LEFT JOIN DRVOM2023   om   ON h.UNITID = om.UNITID
        LEFT JOIN DRVAL2023   lib  ON h.UNITID = lib.UNITID
    """,
}


def _load_access_tables(acc_path: str, skip: set, db, label: str):
    """Load all data tables from one Access database into DuckDB."""
    acc = pyodbc.connect(
        f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={acc_path};"
    )
    cur = acc.cursor()
    tables = sorted(
        r.table_name for r in cur.tables(tableType="TABLE")
        if r.table_name not in skip
        and r.table_name.lower() not in {s.lower() for s in skip}
    )
    print(f"\n  {label}: {len(tables)} tables found")
    for tbl in tables:
        print(f"    {tbl:<35}", end="", flush=True)
        cur.execute(f"SELECT * FROM [{tbl}]")
        cols = [d[0].upper() for d in cur.description]
        rows = cur.fetchall()
        df   = pd.DataFrame.from_records(rows, columns=cols)
        for col in df.columns:
            if df[col].dtype.kind in ("i", "u"):
                df[col] = df[col].astype("int64")
            elif df[col].dtype.kind == "f":
                df[col] = df[col].astype("float64")
            elif df[col].dtype == object:
                sample = df[col].dropna()
                if len(sample) > 0 and isinstance(sample.iloc[0], decimal.Decimal):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
        tname = tbl.upper().replace("-", "_")
        db.execute(f'DROP TABLE IF EXISTS "{tname}"')
        db.register("_tmp", df)
        db.execute(f'CREATE TABLE "{tname}" AS SELECT * FROM _tmp')
        db.unregister("_tmp")
        print(f"{len(df):>10,} rows")
    acc.close()


def main():
    if os.path.exists(DB):
        os.remove(DB)
        print("Removed existing database.")

    db = duckdb.connect(DB)

    # ── Step 1: load all raw tables from each year ────────────────────────────
    print("=" * 60)
    print("Loading raw tables from Access databases ...")
    for yr in YEARS:
        _load_access_tables(yr["accdb"], yr["skip"], db, yr["label"])

    # ── Step 2: load metadata from the latest year's Excel ───────────────────
    print("\nLoading metadata from Excel ...")
    for sheet, name in [("varTable24", "META_VARS"), ("valueSets24", "META_VALUES")]:
        df = pd.read_excel(META_EXCEL, sheet_name=sheet, engine="openpyxl")
        df.columns = [c.strip().upper() for c in df.columns]
        db.execute(f'DROP TABLE IF EXISTS "{name}"')
        db.register("_tmp", df)
        db.execute(f'CREATE TABLE "{name}" AS SELECT * FROM _tmp')
        db.unregister("_tmp")
        print(f"  {sheet}: {len(df):,} rows -> {name}")

    # ── Step 3: build METRICS_LONG ───────────────────────────────────────────
    print("\nBuilding METRICS_LONG ...")
    db.execute("DROP TABLE IF EXISTS METRICS_LONG")
    union_sql = "\nUNION ALL\n".join(METRICS_SQL[yr["label"]] for yr in YEARS)
    db.execute(f"CREATE TABLE METRICS_LONG AS {union_sql}")
    n = db.execute("SELECT COUNT(*) FROM METRICS_LONG").fetchone()[0]
    years_in = db.execute("SELECT YEAR, COUNT(*) FROM METRICS_LONG GROUP BY YEAR ORDER BY YEAR").fetchall()
    print(f"  METRICS_LONG: {n:,} rows total")
    for yr_lbl, cnt in years_in:
        print(f"    {yr_lbl}: {cnt:,} institutions")

    db.close()
    size_mb = os.path.getsize(DB) / 1024 / 1024
    print(f"\nDatabase ready: {DB}  ({size_mb:.0f} MB)")


if __name__ == "__main__":
    main()
