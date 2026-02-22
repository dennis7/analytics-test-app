from pathlib import Path

from sqlmesh import macro


def _read_sql(name: str) -> str:
    sql_path = Path(__file__).parent / f"{name}.sql"
    return sql_path.read_text()


@macro()
def shared_prep_public_securities(evaluator):
    return _read_sql("prep_public_securities")


@macro()
def shared_prep_private_securities(evaluator):
    return _read_sql("prep_private_securities")


@macro()
def shared_harm_valued_positions(evaluator, metric: str):
    return _read_sql("harm_valued_positions").format(metric=metric)


@macro()
def shared_harm_portfolio_weights(evaluator, metric: str):
    return _read_sql("harm_portfolio_weights").format(metric=metric)


@macro()
def reporting_date_filter(evaluator):
    rd = evaluator.var("reporting_date")
    if rd:
        return f"as_of_date = CAST('{rd}' AS DATE)"
    return "TRUE"
