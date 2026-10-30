import pandas as pd

from fraud.evaluate import evaluate, format_report, precision_recall


def test_precision_recall():
    predicted = pd.Series([True, True, True, False, False])
    actual = pd.Series([True, True, False, True, False])
    m = precision_recall(predicted, actual)
    assert (m["true_positives"], m["false_positives"], m["missed"]) == (2, 1, 1)
    assert m["precision"] == 2 / 3
    assert m["recall"] == 2 / 3


def test_nothing_flagged_gives_zero_not_error():
    m = precision_recall(pd.Series([False, False]), pd.Series([True, False]))
    assert m["precision"] == 0.0 and m["recall"] == 0.0


def test_evaluate_joins_labels_by_transaction_id():
    scored = pd.DataFrame({"transaction_id": ["a", "b", "c"],
                           "rules_triggered": ["new_device; unusual_hour", "new_device", ""],
                           "reasons": ["", "", ""],
                           "risk_score": [2, 1, 0]})
    labels = pd.Series({"c": False, "b": False, "a": True})          # different order on purpose
    result = evaluate(scored, labels, threshold=2)
    assert result["overall"]["precision"] == 1.0
    assert result["overall"]["recall"] == 1.0
    assert result["per_rule"]["new_device"]["flagged"] == 2
    assert result["per_rule"]["new_device"]["precision"] == 0.5
    assert "Precision: 100.0%" in format_report(result)
