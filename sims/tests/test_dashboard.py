"""Guards against the Grafana dashboard drifting from the metrics the gateway actually exports."""

import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
DASHBOARD = ROOT / "deploy" / "grafana" / "dashboards" / "rfq.json"
METRICS_SRC = ROOT / "src" / "metrics" / "prometheus_metrics.cpp"


def exported_metrics() -> set[str]:
    return set(re.findall(r'\.Name\("(rfq_[a-z_]+)"\)', METRICS_SRC.read_text(encoding="utf-8")))


def queried_metrics(dashboard: dict) -> set[str]:
    names = set()
    for panel in dashboard["panels"]:
        for target in panel["targets"]:
            for name in re.findall(r"\brfq_[a-z_]+", target["expr"]):
                names.add(re.sub(r"_(bucket|count|sum)$", "", name))
    return names


def test_dashboard_is_valid_json_with_unique_panel_ids():
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    ids = [p["id"] for p in dashboard["panels"]]
    assert len(ids) == len(set(ids))
    assert dashboard["uid"] == "rfq-overview"


def test_every_queried_metric_is_exported_by_the_gateway():
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    missing = queried_metrics(dashboard) - exported_metrics()
    assert not missing, f"dashboard queries metrics the gateway does not export: {sorted(missing)}"


def test_latency_percentile_panels_exist():
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    exprs = " ".join(t["expr"] for p in dashboard["panels"] for t in p["targets"])
    for quantile in ("0.50", "0.95", "0.99"):
        assert f"histogram_quantile({quantile}" in exprs
