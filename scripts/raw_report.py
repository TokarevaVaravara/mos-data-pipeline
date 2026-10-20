"""Отчёт по raw-партиции (B5 и разбор аномалий): только файлы партиции и лог, без API.

Запуск: python scripts/raw_report.py --load-date 2026-10-21 [--log ~/mos-pipeline-logs/full_load_....log]

"Строки совпадают целиком" - совпадают все поля Cells; внешний Number - позиция
строки в выдаче API, в сравнении не участвует. Перцентили - по ближайшему рангу.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

DATE_FIELDS = ("ValidFrom", "ValidTo", "CancelDate")
RETRY_KINDS = ("500", "429", "другие 5xx", "обрыв / сеть", "таймаут")
EXAMPLES = 5
EXAMPLE_ROWS = 4  # строк одной группы в примерах, остальные - одной строкой "ещё N"


@dataclass
class Partition:
    path: Path
    pages: list[Path]
    rows: list[dict]
    page_of: list[int]  # номер страницы каждой строки
    manifest: dict

    @property
    def cells(self) -> list[dict]:
        return [r["Cells"] for r in self.rows]


def read_partition(path: Path) -> Partition:
    pages = sorted(path.glob("page_*.jsonl"))
    rows, page_of = [], []
    for i, page in enumerate(pages):
        for line in page.read_text(encoding="utf-8").splitlines():
            rows.append(json.loads(line))
            page_of.append(i)
    manifest_path = path / "_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    return Partition(path, pages, rows, page_of, manifest)


# --- вычисления --------------------------------------------------------------

def parse_date(value) -> date | None:
    try:
        return datetime.strptime(value, "%d.%m.%Y").date()
    except (TypeError, ValueError):
        return None


def is_empty(value) -> bool:
    return value in (None, "")


def canon(cells: dict, drop: tuple[str, ...] = ()) -> str:
    return json.dumps({k: v for k, v in cells.items() if k not in drop}, ensure_ascii=False, sort_keys=True)


def percentile(values: list[float], p: float):
    """Ближайший ранг: наименьшее значение, не меньше которого p% выборки."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p / 100 * len(ordered)) - 1)]


def groups_by(keys: list) -> dict:
    """Ключ -> индексы строк, только для ключей, встречающихся больше одного раза."""
    groups = defaultdict(list)
    for i, key in enumerate(keys):
        groups[key].append(i)
    return {k: ix for k, ix in groups.items() if len(ix) > 1}


def dup_summary(cells: list[dict], keys: list) -> dict:
    dups = groups_by(keys)
    same = sum(1 for ix in dups.values() if len({canon(cells[i]) for i in ix}) == 1)
    return {
        "unique": len(set(keys)),
        "dup_keys": len(dups),
        "extra_rows": sum(len(ix) - 1 for ix in dups.values()),
        "same": same,
        "different": len(dups) - same,
    }


def differing_fields(cells: list[dict], groups: dict, drop: tuple[str, ...]) -> Counter:
    """Поле -> число групп, где у строк разные значения этого поля."""
    result = Counter()
    fields = sorted({f for ix in groups.values() for i in ix for f in cells[i]} - set(drop))
    for ix in groups.values():
        for field in fields:
            if len({json.dumps(cells[i].get(field), ensure_ascii=False) for i in ix}) > 1:
                result[field] += 1
    return Counter({f: result.get(f, 0) for f in fields})


def size_buckets(groups: dict) -> Counter:
    return Counter("4+" if len(ix) >= 4 else str(len(ix)) for ix in groups.values())


def overlap(id_groups: dict, pair_groups: dict) -> dict:
    id_rows = {i for ix in id_groups.values() for i in ix}
    pair_rows = {i for ix in pair_groups.values() for i in ix}
    pair_sets = {frozenset(ix) for ix in pair_groups.values()}
    return {
        "rows_in_id_groups": len(id_rows),
        "rows_in_pair_groups": len(pair_rows),
        "rows_in_both": len(id_rows & pair_rows),
        "id_groups_equal_to_pair_group": sum(1 for ix in id_groups.values() if frozenset(ix) in pair_sets),
        "id_groups_without_pair_rows": sum(1 for ix in id_groups.values() if not set(ix) & pair_rows),
        "pair_groups_without_id_rows": sum(1 for ix in pair_groups.values() if not set(ix) & id_rows),
    }


def pick_examples(id_groups: dict, pair_groups: dict, k: int = EXAMPLES) -> list[tuple[str, list[int]]]:
    """Сначала группы пар без повторов ID, затем ID-группы размером 2 и 3, затем самые большие."""
    id_rows = {i for ix in id_groups.values() for i in ix}
    picked = [(f"пара {key}", ix) for key, ix in sorted(pair_groups.items(), key=str) if not set(ix) & id_rows]
    by_size = sorted(id_groups.items(), key=lambda kv: (len(kv[1]), str(kv[0])))
    for size in (2, 3):
        picked += [(f"ID {key}", ix) for key, ix in by_size if len(ix) == size][:1]
    seen = {label for label, _ in picked}
    picked += [(f"ID {key}", ix) for key, ix in reversed(by_size) if f"ID {key}" not in seen]
    return picked[:k]


def retry_counts(log_text: str) -> tuple[Counter, Counter]:
    """(по типам, по исходным сообщениям) для строк '...: <проблема>, повтор через ...'."""
    raw = Counter(m.group(1) for m in re.finditer(r": ([^:\n]+), повтор через", log_text))

    def kind(problem: str) -> str:
        if problem == "HTTP 500":
            return "500"
        if problem == "HTTP 429":
            return "429"
        if problem.startswith("HTTP "):
            return "другие 5xx"
        if "Timeout" in problem or problem == "PageDeadlineExceeded":
            return "таймаут"
        return "обрыв / сеть"

    by_kind = Counter({k: 0 for k in RETRY_KINDS})
    for problem, n in raw.items():
        by_kind[kind(problem)] += n
    return by_kind, raw


def day_stats(values: list[int], p: float) -> list[tuple]:
    if not values:
        return [("строк", 0)]
    return [
        ("строк", len(values)),
        ("min", min(values)),
        ("медиана", statistics.median(values)),
        (f"p{p:g}", percentile(values, p)),
        ("max", max(values)),
    ]


# --- вывод -------------------------------------------------------------------

def table(headers, rows) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(str(x).replace("|", "\\|") for x in r) + " |" for r in rows]
    return "\n".join(lines) + "\n"


def pct(n: int, total: int) -> str:
    return f"{100 * n / total:.2f} %" if total else "-"


def show(value) -> str:
    return repr(value) if is_empty(value) else str(value)


def examples_table(part: Partition, groups: list[tuple], fields: list[str], limit: int | None = None) -> str:
    """groups: [(метка группы, индексы строк)]; limit - сколько строк группы показать."""
    rows = []
    for label, ix in groups:
        for i in ix[:limit]:
            rows.append((label, part.pages[part.page_of[i]].name, part.rows[i].get("Number"),
                         *(show(part.cells[i].get(f)) for f in fields)))
        if limit is not None and len(ix) > limit:
            rows.append((label, f"... ещё {len(ix) - limit} строк", "", *("" for _ in fields)))
    return table(["группа", "страница", "Number в выдаче", *fields], rows)


def report(part: Partition, load_date: date, log_text: str | None) -> str:
    cells, n = part.cells, len(part.rows)
    fields = sorted({f for c in cells for f in c})
    out = [f"# Отчёт по партиции {part.path}\n", f"Файлов страниц: {len(part.pages)}\n"]

    # --- B5 ---
    gid = dup_summary(cells, [r["global_id"] for r in part.rows])
    out += ["## B5. Строки и global_id\n", table(["показатель", "значение"], [
        ("строк", n),
        ("уникальных global_id", gid["unique"]),
        ("global_id с повторами", gid["dup_keys"]),
        ("лишних строк (дублей)", gid["extra_rows"]),
        ("групп, совпадающих целиком", gid["same"]),
        ("групп с различиями", gid["different"]),
        ("внешний global_id != Cells.global_id", sum(1 for r in part.rows if r["global_id"] != r["Cells"].get("global_id"))),
    ])]

    out += ["## B5. _manifest.json\n"]
    if part.manifest:
        out.append(table(["поле", "значение"], [(k, json.dumps(v, ensure_ascii=False)) for k, v in part.manifest.items()]))
    else:
        out.append("_manifest.json нет\n")

    out += ["## B5. Повторы запросов по логу\n"]
    if log_text is None:
        out.append("Лог не указан (--log)\n")
    else:
        by_kind, raw = retry_counts(log_text)
        out.append(table(["тип", "повторов"], [(k, by_kind[k]) for k in RETRY_KINDS]))
        if raw:
            out.append(table(["сообщение в логе", "повторов"], raw.most_common()))

    for field in ("Status", "Zone"):
        counts = Counter(c.get(field) for c in cells)
        out += [f"## B5. {field}\n", table([field, "строк", "доля"], [(show(k), v, pct(v, n)) for k, v in counts.most_common()])]

    dates = {f: [parse_date(c.get(f)) for c in cells] for f in DATE_FIELDS}
    rows = []
    for f, vals in dates.items():
        good = [d for d in vals if d]
        empty = sum(1 for c in cells if is_empty(c.get(f)))
        bad = sum(1 for c, d in zip(cells, vals) if not is_empty(c.get(f)) and d is None)
        rows.append((f, min(good) if good else "-", max(good) if good else "-", empty, bad))
    out += ["## B5. Даты (ДД.ММ.ГГГГ)\n", table(["поле", "минимум", "максимум", "пустых", "непустых, не разбирается"], rows)]

    issued_cancelled = [i for i, c in enumerate(cells) if c.get("Status") == "Выдан" and not is_empty(c.get("CancelDate"))]
    annulled_open = [i for i, c in enumerate(cells) if c.get("Status") == "Аннулирован" and is_empty(c.get("CancelDate"))]
    out += ["## B5. Status и CancelDate\n", table(["условие", "строк"], [
        ('Status = "Выдан" и CancelDate заполнен', len(issued_cancelled)),
        ('Status = "Аннулирован" и CancelDate пуст', len(annulled_open)),
    ])]

    vf, vt, cd = dates["ValidFrom"], dates["ValidTo"], dates["CancelDate"]
    cancel_before = [i for i in range(n) if vf[i] and cd[i] and cd[i] < vf[i]]
    out += ["## B5. Порядок дат\n", table(["условие", "строк"], [
        ("ValidTo < ValidFrom", sum(1 for a, b in zip(vf, vt) if a and b and b < a)),
        ("CancelDate < ValidFrom", len(cancel_before)),
    ])]

    id_keys = [c.get("ID") for c in cells]
    pair_keys = [(c.get("Series"), c.get("Number")) for c in cells]
    out += ["## B5. Повторы ID и (Series, Number)\n", table(
        ["ключ", "уникальных", "ключей с повторами", "лишних строк", "групп, совпадающих целиком", "групп с различиями"],
        [(name, *dup_summary(cells, keys).values()) for name, keys in (("ID", id_keys), ("(Series, Number)", pair_keys))],
    )]

    tail = range(max(0, len(part.pages) - 3), len(part.pages))
    gid_pages = defaultdict(set)
    for r, pg in zip(part.rows, part.page_of):
        gid_pages[r["global_id"]].add(pg)
    rows = []
    for pg in tail:
        ids = [r["global_id"] for r, p in zip(part.rows, part.page_of) if p == pg]
        rows.append((part.pages[pg].name, len(ids), len(ids) - len(set(ids)), sum(1 for g in set(ids) if gid_pages[g] - {pg})))
    out += ["## B5. Последняя и две предыдущие страницы\n",
            table(["страница", "строк", "дублей global_id внутри", "global_id, встречающихся на других страницах"], rows)]

    # --- 1. группы повторов ---
    id_groups, pair_groups = groups_by(id_keys), groups_by(pair_keys)
    id_drop, pair_drop = ("global_id",), ("global_id", "ID")
    out += ["## 1. Группы повторов ID и (Series, Number)\n"]
    for title, groups, drop in (("ID", id_groups, id_drop), ("(Series, Number)", pair_groups, pair_drop)):
        same = sum(1 for ix in groups.values() if len({canon(cells[i], drop) for i in ix}) == 1)
        out += [
            f"### {title}: групп {len(groups)}, сравнение без {', '.join(drop)}\n",
            table(["поле", "групп, где различается"], differing_fields(cells, groups, drop).most_common()),
            f"Групп, где строки совпадают по всем полям, кроме {', '.join(drop)}: {same} из {len(groups)}\n",
            table(["размер группы", "групп"], [(k, size_buckets(groups).get(k, 0)) for k in ("2", "3", "4+")]),
            f"Максимальный размер группы: {max((len(ix) for ix in groups.values()), default=0)}\n",
        ]
    ov = overlap(id_groups, pair_groups)
    out += ["### Пересечение групп ID и групп (Series, Number)\n", table(["показатель", "значение"], [
        ("строк в группах ID", ov["rows_in_id_groups"]),
        ("строк в группах (Series, Number)", ov["rows_in_pair_groups"]),
        ("строк в обоих", ov["rows_in_both"]),
        ("групп ID, совпадающих по составу с группой пары", ov["id_groups_equal_to_pair_group"]),
        ("групп ID без строк из групп пар", ov["id_groups_without_pair_rows"]),
        ("групп пар без строк из групп ID", ov["pair_groups_without_id_rows"]),
    ])]
    sample = pick_examples(id_groups, pair_groups)
    out += [f"### Примеры: {len(sample)} групп (пары без повторов ID, ID-группы размером 2 и 3, самые большие)\n",
            examples_table(part, sample, fields, limit=EXAMPLE_ROWS)]

    # --- 2. CancelDate < ValidFrom ---
    gaps = [(vf[i] - cd[i]).days for i in cancel_before]
    future = sum(1 for i in cancel_before if vf[i] > load_date)
    zones = Counter(cells[i].get("Zone") for i in cancel_before)
    out += ["## 2. CancelDate < ValidFrom\n",
            table(["разрыв ValidFrom - CancelDate, дней", "значение"], day_stats(gaps, 90)),
            table(["показатель", "значение"], [
                (f"ValidFrom позже {load_date.isoformat()}", f"{future} ({pct(future, len(cancel_before))})"),
            ]),
            table(["Zone", "строк", "доля"], [(show(z), v, pct(v, len(cancel_before))) for z, v in zones.most_common()])]

    # --- 3. длительность ---
    durations = [(b - a).days for a, b in zip(vf, vt) if a and b]
    out += ["## 3. Длительность ValidTo - ValidFrom, дней\n",
            table(["показатель", "значение"], day_stats(durations, 99) + [
                ("длиннее 400 дней", sum(1 for d in durations if d > 400)),
            ]),
            table(["длительность, дней", "строк", "доля"], [(d, v, pct(v, len(durations))) for d, v in Counter(durations).most_common(10)])]

    # --- 4. Status / CancelDate ---
    out += ["## 4. Несоответствие Status и CancelDate\n"]
    for title, ix in (('"Выдан" с CancelDate', issued_cancelled), ('"Аннулирован" без CancelDate', annulled_open)):
        out += [f"### {title}: {len(ix)} строк, примеры\n",
                examples_table(part, [(str(k + 1), [i]) for k, i in enumerate(ix[:EXAMPLES])], fields)]

    return "\n".join(out)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Отчёт по raw-партиции, без запросов к API")
    parser.add_argument("--load-date", type=date.fromisoformat, required=True)
    parser.add_argument("--raw-root", type=Path, default=Path(os.environ.get("MOS_RAW_ROOT", "data/raw")))
    parser.add_argument("--dataset", default="permits")
    parser.add_argument("--log", type=Path, help="лог загрузки: из него считаются повторы запросов")
    args = parser.parse_args(argv)

    path = args.raw_root / args.dataset / f"load_date={args.load_date.isoformat()}"
    if not path.is_dir():
        parser.error(f"нет партиции {path}")
    log_text = args.log.read_text(encoding="utf-8") if args.log else None
    sys.stdout.write(report(read_partition(path), args.load_date, log_text))


if __name__ == "__main__":
    main()
