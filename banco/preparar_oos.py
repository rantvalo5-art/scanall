"""Congela el universo de prueba del PREREGISTRO_OI: simbolos 41-100, sin los 40 usados."""
import json
import os

from klines import CACHE, _universo_fijo

HERE = os.path.dirname(os.path.abspath(__file__))

usados = json.load(open(os.path.join(CACHE, "universo_metricas40.json"), encoding="utf-8"))
todos = _universo_fijo(100, "metricas100")
nuevos = [s for s in todos if s not in usados]

p = os.path.join(CACHE, "universo_oos_oi.json")
json.dump(nuevos, open(p, "w", encoding="utf-8"))
print(f"usados: {len(usados)} | top-100: {len(todos)} | NUEVOS para el test: {len(nuevos)}")
print(", ".join(nuevos[:12]), "...")
