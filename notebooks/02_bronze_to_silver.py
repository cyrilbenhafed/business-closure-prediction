import marimo

__generated_with = "0.17.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    from pathlib import Path
    import polars as pl
    from datetime import datetime
    import marimo as mo
    return Path, mo, pl, sys


@app.cell
def _(Path, mo, sys):
    project_root = Path(__file__).parent.parent
    sys.path.append(str(project_root / "src"))

    from utils.config import get_data_paths, load_config

    mo.md("# 🔄 Bronze to Silver Transformation")
    return (get_data_paths,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Configuration
    """)
    return


@app.cell
def _(pl):
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_rows(10)
    pl.Config.set_tbl_width_chars(1000)
    return


@app.cell
def _(Path, get_data_paths):
    data_paths = get_data_paths()
    BRONZE_PATH = Path(data_paths["bronze"])
    SILVER_PATH = Path(data_paths["silver"])
    SILVER_PATH.mkdir(parents=True, exist_ok=True)
    return (BRONZE_PATH,)


@app.cell
def _(BRONZE_PATH, pl):
    brz_files = {}
    for file in list(BRONZE_PATH.glob("*.parquet")):
        brz_files[file.stem] = pl.scan_parquet(file)
    clean_files = {}  # Will be used to store cleaned data
    return brz_files, clean_files


@app.cell
def _(mo):
    mo.md(r"""
    ## Utils
    """)
    return


@app.cell
def _(pl):
    def print_reduction_stats(bronze_df, clean_df):
        """Quick helper to show reduction statistics between raw and clean data."""
        # Get stats
        bronze_rows = bronze_df.select(pl.len()).collect().item()
        clean_rows = clean_df.select(pl.len()).collect().item()

        bronze_cols = len(bronze_df.collect_schema())
        clean_cols = len(clean_df.collect_schema())

        # Calculate changes
        removed_rows = bronze_rows - clean_rows
        retention_pct = (clean_rows / bronze_rows) * 100 if bronze_rows > 0 else 0

        removed_cols = bronze_cols - clean_cols

        # Print summary
        print(f"\nSummary:")
        print(
            f"  Rows:    {bronze_rows:,} → {clean_rows:,} ({removed_rows:,} removed, {retention_pct:.1f}% retained)"
        )
        print(
            f"  Columns: {bronze_cols} → {clean_cols} ({removed_cols:d} columns removed)"
        )
    return (print_reduction_stats,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Scope delimitation
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Siren cleaning

    We start by filtering on StockUniteLegale file as it will dramatically reduce the number of rows in all datasets. Siren represents a company in INSEE data modeling. As stated before, we want to include only legal entities (no physical person). To do so, we have to keep only record where nomUniteLegale is empty
    """)
    return


@app.cell
def _(brz_files, clean_files, pl):
    clean_files["StockUniteLegale_utf8"] = brz_files[
        "StockUniteLegale_utf8"
    ].filter(pl.col("nomUniteLegale").is_null())
    return


@app.cell
def _(mo):
    mo.md(r"""
    Some companies do not share their data, generally because of sensitive information (military market, ect.). We will remove them from the scope.
    """)
    return


@app.cell
def _(clean_files, pl):
    clean_files["StockUniteLegale_utf8"] = clean_files[
        "StockUniteLegale_utf8"
    ].filter(pl.col("statutDiffusionUniteLegale") == "O")
    return


@app.cell
def _(mo):
    mo.md(r"""
    Finally, companies which have been closed before 31/12/2002 are identified with unitePurgeeUniteLegale set to true. This represents obsolete data we are not interested in. We will see in this notebook that we will even narrow down the date range of our project.
    """)
    return


@app.cell
def _(clean_files, pl):
    clean_files["StockUniteLegale_utf8"] = clean_files[
        "StockUniteLegale_utf8"
    ].filter(pl.col("unitePurgeeUniteLegale").is_null())
    return


@app.cell
def _(mo):
    mo.md(r"""
    Finally we will pick only relevant columns
    """)
    return


@app.cell
def _(clean_files):
    clean_files["StockUniteLegale_utf8"] = clean_files[
        "StockUniteLegale_utf8"
    ].select(
        [
            "siren",
            "dateCreationUniteLegale",
            "identifiantAssociationUniteLegale",
            "trancheEffectifsUniteLegale",
            "anneeEffectifsUniteLegale",
            "dateDernierTraitementUniteLegale",
            "nombrePeriodesUniteLegale",
            "categorieEntreprise",
            "anneeCategorieEntreprise",
            "dateDebut",
            "etatAdministratifUniteLegale",
            "denominationUniteLegale",
            "denominationUsuelle1UniteLegale",
            "denominationUsuelle2UniteLegale",
            "denominationUsuelle3UniteLegale",
            "categorieJuridiqueUniteLegale",
            "activitePrincipaleUniteLegale",
            "nomenclatureActivitePrincipaleUniteLegale",
            "economieSocialeSolidaireUniteLegale",
            "societeMissionUniteLegale",
            "caractereEmployeurUniteLegale",
        ]
    )
    return


@app.cell
def _(brz_files, clean_files, print_reduction_stats):
    print_reduction_stats(
        brz_files["StockUniteLegale_utf8"], clean_files["StockUniteLegale_utf8"]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    And now for the history file :
    """)
    return


@app.cell
def _(brz_files, clean_files):
    clean_files["StockUniteLegaleHistorique_utf8"] = brz_files[
        "StockUniteLegaleHistorique_utf8"
    ].join(clean_files["StockUniteLegale_utf8"], on="siren", how="semi")
    return


@app.cell
def _(brz_files, clean_files, print_reduction_stats):
    print_reduction_stats(
        brz_files["StockUniteLegaleHistorique_utf8"],
        clean_files["StockUniteLegaleHistorique_utf8"],
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Siret cleaning

    Now let's move to siret (etablissement) : siret represents the physical establishment (location) of a company in INSEE data modeling. One siren can have multiple siret.
    We simply remove all siret linked to siren that have been filtered out.
    """)
    return


@app.cell
def _(brz_files, clean_files):
    clean_files["StockEtablissement_utf8"] = brz_files[
        "StockEtablissement_utf8"
    ].join(clean_files["StockUniteLegale_utf8"], on="siren", how="semi")
    return


@app.cell
def _(clean_files):
    clean_files["StockEtablissement_utf8"] = clean_files[
        "StockEtablissement_utf8"
    ].select(
        [
            "siret",
            "statutDiffusionEtablissement",
            "dateCreationEtablissement",
            "trancheEffectifsEtablissement",
            "anneeEffectifsEtablissement",
            "activitePrincipaleRegistreMetiersEtablissement",
            "dateDernierTraitementEtablissement",
            "etablissementSiege",
            "nombrePeriodesEtablissement",
            "typeVoieEtablissement",
            "codePostalEtablissement",
            "libelleCommuneEtablissement",
            "libelleCommuneEtrangerEtablissement",
            "distributionSpecialeEtablissement",
            "codeCommuneEtablissement",
            "codeCedexEtablissement",
            "libelleCedexEtablissement",
            "codePaysEtrangerEtablissement",
            "libellePaysEtrangerEtablissement",
            "identifiantAdresseEtablissement",
            "coordonneeLambertAbscisseEtablissement",
            "coordonneeLambertOrdonneeEtablissement",
            "dateDebut",
            "etatAdministratifEtablissement",
            "enseigne1Etablissement",
            "enseigne2Etablissement",
            "enseigne3Etablissement",
            "denominationUsuelleEtablissement",
            "activitePrincipaleEtablissement",
            "nomenclatureActivitePrincipaleEtablissement",
            "caractereEmployeurEtablissement",
        ]
    )
    return


@app.cell
def _(brz_files, clean_files, print_reduction_stats):
    print_reduction_stats(
        brz_files["StockEtablissement_utf8"],
        clean_files["StockEtablissement_utf8"],
    )
    return


@app.cell
def _(brz_files, clean_files):
    clean_files["StockEtablissementHistorique_utf8"] = brz_files[
        "StockEtablissementHistorique_utf8"
    ].join(clean_files["StockEtablissement_utf8"], on="siret", how="semi")
    return


@app.cell
def _(clean_files):
    clean_files["StockEtablissementHistorique_utf8"] = clean_files[
        "StockEtablissementHistorique_utf8"
    ].select(
        [
            "siret",
            "dateFin",
            "dateDebut",
            "etatAdministratifEtablissement",
            "changementEtatAdministratifEtablissement",
            "enseigne1Etablissement",
            "enseigne2Etablissement",
            "enseigne3Etablissement",
            "changementEnseigneEtablissement",
            "denominationUsuelleEtablissement",
            "changementDenominationUsuelleEtablissement",
            "activitePrincipaleEtablissement",
            "nomenclatureActivitePrincipaleEtablissement",
            "changementActivitePrincipaleEtablissement",
            "caractereEmployeurEtablissement",
            "changementCaractereEmployeurEtablissement",
        ]
    )
    return


@app.cell
def _(brz_files, clean_files, print_reduction_stats):
    print_reduction_stats(
        brz_files["StockEtablissementHistorique_utf8"],
        clean_files["StockEtablissementHistorique_utf8"],
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
