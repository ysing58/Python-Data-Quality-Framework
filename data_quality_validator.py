"""
Python Data Quality Framework
- Validates data quality rules on pandas DataFrames
- Generates summary reports
- Extensible rule definitions
"""
from __future__ import annotations
import pandas as pd
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Any


@dataclass
class RuleResult:
    name: str
    passed: bool
    failures: int
    total: int
    details: Optional[pd.DataFrame] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "failures": self.failures,
            "total": self.total,
        }


class DataQualityRule:
    def __init__(self, name: str, func: Callable[[pd.DataFrame], pd.Series], description: str = ""):
        self.name = name
        self.func = func
        self.description = description

    def evaluate(self, df: pd.DataFrame) -> RuleResult:
        mask = self.func(df)
        if not isinstance(mask, pd.Series) or mask.dtype != bool:
            raise ValueError("Rule function must return a boolean pandas Series")
        total = len(df)
        failures_mask = ~mask
        failures = int(failures_mask.sum())
        details = df.loc[failures_mask].copy()
        return RuleResult(name=self.name, passed=failures == 0, failures=failures, total=total, details=details)


class DataQualityValidator:
    def __init__(self, rules: Optional[List[DataQualityRule]] = None):
        self.rules: List[DataQualityRule] = rules or []

    def add_rule(self, rule: DataQualityRule) -> None:
        self.rules.append(rule)

    def validate(self, df: pd.DataFrame) -> List[RuleResult]:
        results: List[RuleResult] = []
        for rule in self.rules:
            result = rule.evaluate(df)
            results.append(result)
        return results

    @staticmethod
    def summary(results: List[RuleResult]) -> pd.DataFrame:
        return pd.DataFrame([r.to_dict() for r in results])


# Built-in rule helpers

def not_null(columns: List[str]) -> DataQualityRule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[columns].notnull().all(axis=1)
    return DataQualityRule(name=f"not_null({','.join(columns)})", func=_rule, description="No NULLs in required columns")


def in_range(column: str, min_value: float, max_value: float, inclusive: bool = True) -> DataQualityRule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        if inclusive:
            return df[column].between(min_value, max_value, inclusive="both")
        return (df[column] > min_value) & (df[column] < max_value)
    return DataQualityRule(name=f"in_range({column},{min_value},{max_value})", func=_rule, description="Numeric range validation")


def unique(column: str) -> DataQualityRule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return ~df[column].duplicated(keep=False)
    return DataQualityRule(name=f"unique({column})", func=_rule, description="Uniqueness constraint")


def matches_regex(column: str, pattern: str) -> DataQualityRule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[column].astype(str).str.match(pattern, na=False)
    return DataQualityRule(name=f"matches_regex({column})", func=_rule, description="Regex pattern match")


if __name__ == "__main__":
    # Sample usage
    data = pd.DataFrame({
        "customer_id": [1, 2, 2, 4],
        "email": ["a@example.com", "bad_email", "b@example.com", None],
        "age": [25, 17, 130, 40],
    })

    validator = DataQualityValidator([
        not_null(["customer_id", "email"]),
        unique("customer_id"),
        matches_regex("email", r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$"),
        in_range("age", 18, 120),
    ])

    results = validator.validate(data)
    print(DataQualityValidator.summary(results))
    for res in results:
        if not res.passed:
            print(f"Rule failed: {res.name} - {res.failures}/{res.total} failures")
            print(res.details)
