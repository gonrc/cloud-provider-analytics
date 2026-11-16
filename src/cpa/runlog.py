"""Identificador y registro de cada corrida.

Cada paso del pipeline recibe un `Run`: el `run_id` queda como columna en cada
fila que escribe (linaje) y los conteos de la corrida se guardan en
evidence/runs/<run_id>.json (observabilidad).
"""
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class Run:
    step: str
    evidence_dir: str
    run_id: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6])
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))
    counts: dict = field(default_factory=dict)
    checks: dict = field(default_factory=dict)

    @property
    def ingest_date(self) -> str:
        return self.started_at[:10]

    def count(self, table: str, **values) -> None:
        self.counts.setdefault(table, {}).update(values)

    def check(self, name: str, ok: bool, detail: str) -> None:
        self.checks[name] = {"ok": bool(ok), "detail": detail}

    def save(self) -> Path:
        out = Path(self.evidence_dir) / "runs" / f"{self.run_id}_{self.step}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "run_id": self.run_id,
            "step": self.step,
            "started_at": self.started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "counts": self.counts,
            "checks": self.checks,
        }
        out.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        return out
