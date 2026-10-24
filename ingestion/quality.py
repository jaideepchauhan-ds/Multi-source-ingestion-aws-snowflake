"""Data-quality gate. A source that fails any rule is NOT uploaded and raises an alert,
so bad data never reaches Snowflake (fail fast, quarantine at the edge)."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date

import pandas as pd

RowCheck = Callable[[pd.DataFrame], pd.Series]            # returns True where a row VIOLATES the rule
DatasetCheck = Callable[[pd.DataFrame, date], "str | None"]  # returns an error message or None


@dataclass(frozen=True)
class Rules:
    columns: list[str]
    key_cols: list[str]
    not_null: list[str]
    min_rows: int = 1
    row_checks: dict[str, RowCheck] = field(default_factory=dict)
    dataset_checks: list[DatasetCheck] = field(default_factory=list)


def freshness(column: str, max_age_days: int) -> DatasetCheck:
    def check(df: pd.DataFrame, run_date: date) -> str | None:
        if df.empty:
            return None
        latest = pd.to_datetime(df[column]).max().date()
        age = (run_date - latest).days
        if age > max_age_days:
            return f"stale_data: latest {column}={latest} is {age} days old (max {max_age_days})"
        return None

    return check


def validate(df: pd.DataFrame, rules: Rules, run_date: date) -> list[str]:
    failures: list[str] = []

    if list(df.columns) != rules.columns:
        return [f"schema_drift: expected {rules.columns}, got {list(df.columns)}"]

    if len(df) < rules.min_rows:
        failures.append(f"row_count: {len(df)} rows, expected at least {rules.min_rows}")
    if df.empty:
        return failures

    for col in rules.not_null:
        nulls = int(df[col].isna().sum())
        if nulls:
            failures.append(f"null_values: {nulls} nulls in '{col}'")

    dupes = int(df.duplicated(subset=rules.key_cols).sum())
    if dupes:
        failures.append(f"duplicate_keys: {dupes} duplicate rows on {rules.key_cols}")

    for name, check in rules.row_checks.items():
        bad = int(check(df).sum())
        if bad:
            failures.append(f"{name}: {bad} violating rows")

    for dataset_check in rules.dataset_checks:
        message = dataset_check(df, run_date)
        if message:
            failures.append(message)

    return failures
