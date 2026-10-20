"""scripts/raw_report.py на маленькой партиции с заранее известными дублями и аномалиями."""
import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "raw_report.py"
spec = importlib.util.spec_from_file_location("raw_report", SCRIPT)
rr = importlib.util.module_from_spec(spec)
sys.modules["raw_report"] = rr  # dataclass ищет модуль по имени
spec.loader.exec_module(rr)

DAY = date(2026, 10, 21)

#   gid  ID   Series Number  ValidFrom     ValidTo       Zone   Status         CancelDate
ROWS = [
    (1, 100, "БА", "0001", "01.01.2020", "31.12.2020", "СК", "Выдан", ""),  # 0
    (2, 101, "БА", "0002", "01.01.2020", "31.12.2020", "ТТК", "Аннулирован", "01.06.2020"),  # 1
    (1, 100, "БА", "0001", "01.01.2020", "31.12.2020", "СК", "Выдан", ""),  # 2: полный дубль строки 0
    (3, 102, "БА", "0003", "01.01.2020", "31.12.2020", "СК", "Выдан", "05.05.2020"),  # 3: Выдан с CancelDate
    (4, 103, "БА", "0004", "01.01.2020", "31.12.2020", "СК", "Аннулирован", ""),  # 4: Аннулирован без CancelDate
    (5, 101, "БА", "0002", "01.01.2020", "31.12.2020", "ТТК", "Выдан", ""),  # 5: тот же ID и пара, что у 1
    (6, 104, "БА", "0005", "10.01.2030", "09.01.2031", "МКАД", "Аннулирован", "01.01.2026"),  # 6: отмена до начала
    (7, 105, "ВВ", "0006", "01.03.2021", "01.02.2021", "СК", "Выдан", ""),  # 7: ValidTo < ValidFrom
    (8, 106, "ВВ", "0007", "2021-03-01", "01.03.2022", "СК", "Выдан", ""),  # 8: дата не ДД.ММ.ГГГГ
    (9, 107, "ГГ", "0008", "01.01.2021", "31.12.2021", "СК", "Выдан", ""),  # 9-11: одна пара, разные ID
    (10, 108, "ГГ", "0008", "01.01.2021", "31.12.2021", "СК", "Выдан", ""),
    (11, 109, "ГГ", "0008", "01.01.2021", "31.12.2021", "МКАД", "Выдан", ""),
    (12, 110, "ДД", "0009", "01.01.2020", "01.03.2021", "СК", "Выдан", ""),  # 12: 425 дней
]
FIELDS = ("global_id", "ID", "Series", "Number", "ValidFrom", "ValidTo", "Zone", "Status", "CancelDate")

LOG = """\
2026-10-21 00:13:32,127 WARNING loader.api_loader: продолжить нельзя (файла прогресса нет): загрузка с нуля
2026-10-21 00:19:17,126 WARNING loader.api_client: GET /datasets/3247/rows {'$skip': 67000, '$top': 1000}: ConnectionError, повтор через 5.0 с
2026-10-21 00:22:49,879 WARNING loader.api_client: GET /datasets/3247/rows {'$skip': 114000, '$top': 1000}: HTTP 502, повтор через 5.0 с
2026-10-21 00:44:33,525 WARNING loader.api_client: GET /datasets/3247/rows {'$skip': 357000, '$top': 1000}: HTTP 500, повтор через 5.0 с
2026-10-21 00:46:20,757 WARNING loader.api_client: GET /datasets/3247/rows {'$skip': 378000, '$top': 1000}: HTTP 500, повтор через 5.0 с
2026-10-21 01:09:18,616 WARNING loader.api_client: GET /datasets/3247/rows {'$skip': 585000, '$top': 1000}: ReadTimeout, повтор через 5.0 с
2026-10-21 01:10:00,000 WARNING loader.api_client: GET /datasets/3247/rows {'$skip': 586000, '$top': 1000}: PageDeadlineExceeded, повтор через 5.0 с
2026-10-21 01:46:28,194 WARNING loader.api_loader: нехватка 1 строк в пределах допуска 10
"""


@pytest.fixture
def raw_root(tmp_path):
    part = tmp_path / "raw" / "permits" / f"load_date={DAY.isoformat()}"
    part.mkdir(parents=True)
    rows = []
    for n, values in enumerate(ROWS, start=1):
        cells = dict(zip(FIELDS, values))
        rows.append({"global_id": cells["global_id"], "Number": n, "Cells": cells})
    for page, chunk in enumerate((rows[:7], rows[7:])):
        (part / f"page_{page:06d}.jsonl").write_text(
            "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in chunk), encoding="utf-8"
        )
    (part / "_manifest.json").write_text(json.dumps({"rows": len(rows), "shortfall": 0}), encoding="utf-8")
    return tmp_path / "raw"


@pytest.fixture
def part(raw_root):
    return rr.read_partition(raw_root / "permits" / f"load_date={DAY.isoformat()}")


def test_global_id_duplicates(part):
    s = rr.dup_summary(part.cells, [r["global_id"] for r in part.rows])
    assert s == {"unique": 12, "dup_keys": 1, "extra_rows": 1, "same": 1, "different": 0}


def test_id_and_pair_groups(part):
    cells = part.cells
    id_groups = rr.groups_by([c["ID"] for c in cells])
    pair_groups = rr.groups_by([(c["Series"], c["Number"]) for c in cells])
    assert id_groups == {100: [0, 2], 101: [1, 5]}
    assert pair_groups == {("БА", "0001"): [0, 2], ("БА", "0002"): [1, 5], ("ГГ", "0008"): [9, 10, 11]}

    by_id = rr.differing_fields(cells, id_groups, ("global_id",))
    assert "global_id" not in by_id
    assert {f: n for f, n in by_id.items() if n} == {"Status": 1, "CancelDate": 1}

    by_pair = rr.differing_fields(cells, pair_groups, ("global_id", "ID"))
    assert "ID" not in by_pair
    assert {f: n for f, n in by_pair.items() if n} == {"Status": 1, "CancelDate": 1, "Zone": 1}

    assert rr.size_buckets(pair_groups) == {"2": 2, "3": 1}
    assert rr.overlap(id_groups, pair_groups) == {
        "rows_in_id_groups": 4,
        "rows_in_pair_groups": 7,
        "rows_in_both": 4,
        "id_groups_equal_to_pair_group": 2,
        "id_groups_without_pair_rows": 0,
        "pair_groups_without_id_rows": 1,
    }


def test_examples_put_pair_without_id_duplicates_first(part):
    cells = part.cells
    id_groups = rr.groups_by([c["ID"] for c in cells])
    pair_groups = rr.groups_by([(c["Series"], c["Number"]) for c in cells])
    labels = [label for label, _ in rr.pick_examples(id_groups, pair_groups)]
    assert labels == ["пара ('ГГ', '0008')", "ID 100", "ID 101"]


def test_examples_table_truncates_long_groups(part):
    text = rr.examples_table(part, [("g", list(range(6)))], ["ID"], limit=4)
    assert text.count("| g |") == 5 and "... ещё 2 строк" in text


def test_percentile_nearest_rank():
    assert rr.percentile(list(range(1, 11)), 90) == 9
    assert rr.percentile(list(range(1, 101)), 99) == 99
    assert rr.percentile([5], 50) == 5


def test_retry_counts_by_kind():
    by_kind, raw = rr.retry_counts(LOG)
    assert by_kind == {"500": 2, "429": 0, "другие 5xx": 1, "обрыв / сеть": 1, "таймаут": 2}
    assert sum(raw.values()) == 6  # строки без "повтор через" не считаются


def test_report_numbers(part):
    text = rr.report(part, DAY, LOG)
    assert "| строк | 13 |" in text
    assert "| ValidTo < ValidFrom | 1 |" in text
    assert "| CancelDate < ValidFrom | 1 |" in text
    assert "| ValidFrom | 2020-01-01 | 2030-01-10 | 0 | 1 |" in text  # одна дата в формате ГГГГ-ММ-ДД
    assert '| Status = "Выдан" и CancelDate заполнен | 1 |' in text
    assert '| Status = "Аннулирован" и CancelDate пуст | 1 |' in text
    assert "| ID | 11 | 2 | 2 | 1 | 1 |" in text
    assert "| (Series, Number) | 9 | 3 | 4 | 1 | 2 |" in text  # 13 строк - 4 лишних
    # CancelDate < ValidFrom: 01.01.2026 -> 10.01.2030
    gap = (date(2030, 1, 10) - date(2026, 1, 1)).days
    assert f"| min | {gap} |" in text and "| ValidFrom позже 2026-10-21 | 1 (100.00 %) |" in text
    assert "| длиннее 400 дней | 1 |" in text
    assert "### \"Выдан\" с CancelDate: 1 строк, примеры" in text
    assert "### \"Аннулирован\" без CancelDate: 1 строк, примеры" in text
    assert "| page_000001.jsonl | 6 | 0 | 0 |" in text


def test_cli_prints_markdown(raw_root, tmp_path, capsys):
    log = tmp_path / "load.log"
    log.write_text(LOG, encoding="utf-8")
    rr.main(["--load-date", DAY.isoformat(), "--raw-root", str(raw_root), "--log", str(log)])
    out = capsys.readouterr().out
    assert out.startswith("# Отчёт по партиции")
    assert "| таймаут | 2 |" in out


def test_cli_missing_partition(raw_root):
    with pytest.raises(SystemExit):
        rr.main(["--load-date", "2000-01-01", "--raw-root", str(raw_root)])
