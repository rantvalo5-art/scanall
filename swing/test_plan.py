"""
Verificación del plan de trading de la alerta (HANDOFF_plan_trading.md, sección 11).

Dos partes, ambas sin red ni credenciales:
  1. `_build_plan()` (backtest.py) contra dicts sintéticos — los bordes que rompen:
     dist_to_res ausente, major_struct_dist negativo, ref_price 0, precio ya por
     encima del nivel, exit_mgmt apagado, bucket fuera de exit_mgmt.BUCKETS.
  2. `_plan_lines()` (screener.py) — que nunca imprima None/inf/nan y que el bloque
     entre en el caption de sendPhoto (1024 chars, no 4096).

    py -3.13 test_plan.py
"""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from backtest import Config, _build_plan  # noqa: E402

CFG = Config(str(Path(__file__).parent / "config.json"))
FALLA = []


def check(nombre, cond, detalle=""):
    if cond:
        print(f"  ok   {nombre}")
    else:
        print(f"  FALLA {nombre}  {detalle}")
        FALLA.append(nombre)


def cand(**kw):
    base = {"history_tf": "COILING", "timeframe": "4h", "price": 1.0,
            "ref_price": 1.02, "bucket": "BEST"}
    base.update(kw)
    return base


def feats(**kw):
    return {"4h": dict(kw)}


def es_finito(x):
    return x is None or (isinstance(x, float) and math.isfinite(x))


# ── 1. _build_plan ──────────────────────────────────────────────────────────
def test_build_plan():
    print("\n_build_plan()")

    # caso normal: COILING que no rompió, con las dos resistencias arriba
    p = _build_plan(cand(), feats(dist_to_res=0.05, major_struct_dist=0.20), CFG)
    check("COILING arma zona de entrada", p and p["entry_low"] == 1.02)
    check("buffer = PREBREAK_NEAR_MAX",
          p and abs(p["entry_high"] - 1.02 * 1.012) < 1e-12, p and p["entry_high"])
    check("invalidación = ref_price", p and p["invalidation"] == 1.02)
    check("elige la resistencia MÁS CERCANA (1h antes que major)",
          p and p["resistencia_src"] == "1h" and abs(p["resistencia"] - 1.05) < 1e-12)

    # dist_to_res ausente -> cae a major_struct_dist
    p = _build_plan(cand(), feats(dist_to_res=None, major_struct_dist=0.20), CFG)
    check("dist_to_res None cae a major", p and p["resistencia_src"] == "major")

    # las dos ausentes -> sin resistencia, pero el resto del plan sigue
    p = _build_plan(cand(), feats(), CFG)
    check("sin ninguna resistencia -> None (no un número roto)",
          p and p["resistencia"] is None and p["invalidation"] == 1.02)

    # major_struct_dist negativo (precio YA por encima) -> no se muestra
    p = _build_plan(cand(), feats(dist_to_res=-0.03, major_struct_dist=-0.10), CFG)
    check("resistencias por debajo del precio -> None", p and p["resistencia"] is None)

    # resistencia DENTRO de la zona de entrada -> no se muestra
    p = _build_plan(cand(), feats(dist_to_res=0.025, major_struct_dist=0.30), CFG)
    check("resistencia dentro de la zona de entrada se salta",
          p and p["resistencia_src"] == "major")

    # ref_price 0 / ausente -> sin invalidación ni entrada, pero el stop sigue siendo real
    p = _build_plan(cand(ref_price=0), feats(dist_to_res=0.05), CFG)
    check("ref_price 0 -> sin invalidación ni zona",
          p and p["invalidation"] is None and p["entry_low"] is None)
    check("ref_price 0 -> el stop sigue", p and p["stop"] is not None)

    # precio YA por encima del ref (rompió mientras tanto) -> sin zona de entrada
    p = _build_plan(cand(price=1.10), feats(dist_to_res=0.05), CFG)
    check("precio > ref -> sin zona de entrada", p and p["entry_low"] is None)

    # price inválido -> None entero
    check("price 0 -> plan None", _build_plan(cand(price=0), feats(), CFG) is None)

    # señales que YA rompieron: invalidación y stop, nunca entrada ni resistencia
    for sig in ("BREAKOUT", "HOLD", "RIDING"):
        p = _build_plan(cand(history_tf=sig), feats(dist_to_res=0.05,
                                                    major_struct_dist=0.20), CFG)
        check(f"{sig}: sin zona de entrada", p and p["entry_low"] is None)
        check(f"{sig}: sin resistencia", p and p["resistencia"] is None)
        check(f"{sig}: conserva invalidación", p and p["invalidation"] == 1.02)

    # coherencia con exit_tracker: el stop se calcula sobre entry_price (= price)
    ex = CFG.raw.get("exit_mgmt", {})
    p = _build_plan(cand(price=2.0), feats(), CFG)
    check("stop = price*(1-STOP_PCT), igual que exit_tracker",
          p and abs(p["stop"] - 2.0 * (1 - ex["STOP_PCT"])) < 1e-12, p and p["stop"])

    # bucket fuera de exit_mgmt.BUCKETS -> no se imprime un stop que el tracker no aplica
    p = _build_plan(cand(bucket="WATCH"), feats(), CFG)
    check("bucket WATCH -> sin stop", p and p["stop"] is None)

    # exit_mgmt apagado -> tampoco
    apagada = Config.__new__(Config)
    apagada.raw = dict(CFG.raw)
    apagada.path = CFG.path
    apagada.raw["exit_mgmt"] = dict(ex, ENABLED=False)
    p = _build_plan(cand(), feats(), apagada)
    check("exit_mgmt OFF -> sin stop", p and p["stop"] is None)

    # STOP_PCT 0 -> tampoco (por si alguien vuelve a poner 0.0 en config)
    sin_stop = Config.__new__(Config)
    sin_stop.raw = dict(CFG.raw)
    sin_stop.path = CFG.path
    sin_stop.raw["exit_mgmt"] = dict(ex, STOP_PCT=0.0)
    p = _build_plan(cand(), feats(), sin_stop)
    check("STOP_PCT 0 -> sin stop", p and p["stop"] is None)

    # nada que decir -> None (no un dict de Nones que el render tendría que filtrar)
    p = _build_plan(cand(history_tf="BREAKOUT", ref_price=0, bucket="WATCH"),
                    feats(), CFG)
    check("sin ningún nivel -> plan None", p is None)

    # ningún nivel puede ser inf/nan
    for c, f in ((cand(), feats(dist_to_res=0.05)),
                 (cand(price=1e-9), feats(major_struct_dist=1e6)),
                 (cand(ref_price=1e12), feats())):
        p = _build_plan(c, f, CFG)
        if p:
            check("todos los niveles finitos",
                  all(es_finito(v) for k, v in p.items() if k != "resistencia_src"), p)


# ── 2. _plan_lines ──────────────────────────────────────────────────────────
def test_plan_lines():
    print("\n_plan_lines()")
    import os
    os.environ.setdefault("TELEGRAM_TOKEN", "x")
    os.environ.setdefault("TELEGRAM_CHAT_ID", "x")
    os.environ.setdefault("SUPABASE_KEY", "x")
    import screener

    check("sin plan -> sin líneas", screener._plan_lines({"plan": None}) == [])
    check("plan ausente -> sin líneas", screener._plan_lines({}) == [])

    p = _build_plan(cand(price=0.01240, ref_price=0.01249),
                    feats(dist_to_res=0.11, major_struct_dist=0.25), CFG)
    lineas = screener._plan_lines({"plan": p})
    texto = "\n".join(lineas)
    print("\n".join("      " + l for l in lineas))
    check("no imprime None", "None" not in texto)
    check("no imprime inf/nan", "inf" not in texto and "nan" not in texto)
    check("dice `resistencia cercana`, no `objetivo`",
          "resistencia cercana" in texto and "objetivo" not in texto.lower())
    check("no muestra R:B", "r:b" not in texto.lower() and "riesgo/" not in texto.lower())
    check("rótulo del stop = STOP_PCT de config",
          f"-{screener.STOP_PCT_PLAN:.0%}" in texto, texto)

    # markdown de Telegram: nº PAR de _ y de * o el parser rompe el mensaje entero
    check("guiones bajos pares", texto.count("_") % 2 == 0)
    check("asteriscos pares", texto.count("*") % 2 == 0)

    # el caption de sendPhoto son 1024 chars, no 4096
    check(f"bloque corto ({len(texto)} chars)", len(texto) < 200, len(texto))

    # sólo invalidación (BREAKOUT): una línea, sin entrada ni resistencia
    p = _build_plan(cand(history_tf="BREAKOUT"), feats(dist_to_res=0.05), CFG)
    lineas = screener._plan_lines({"plan": p})
    print("\n".join("      " + l for l in lineas))
    check("BREAKOUT: inval + stop y nada más",
          len(lineas) == 2 and "entrada" not in lineas[0]
          and "resistencia" not in "\n".join(lineas))


if __name__ == "__main__":
    test_build_plan()
    test_plan_lines()
    print(f"\n{'TODO OK' if not FALLA else 'FALLAN: ' + ', '.join(FALLA)}")
    sys.exit(1 if FALLA else 0)
