"""
ITEM 4.7 — fadear las senales de extension (EXPLOSION y BREAKOUT).

Es lo UNICO de este repo que cruzo su regla de parada escrita y despues
sobrevivio a todo lo que se le tiro encima. Este script re-corre la evaluacion
completa sobre datos frescos, que es como se hace el forward test:

    $env:SUPABASE_KEY = "<anon key>"    # la de fallback esta vencida (401)
    py -3.13 evaluar.py                 # baja lo que falte y evalua
    py -3.13 evaluar.py --desde 2026-08-17   # SOLO out-of-sample, sin el in-sample

LO QUE FALTA PARA CREERLE: la ventana medida (2026-06-26 -> 08-16) son 51 dias
de UN SOLO regimen bear. Todo lo que brillo en una ventana corta bajista de este
repo murio despues. Hace falta que aguante un tramo alcista.

Las cuatro compuertas son las de la regla escrita en el handoff, mas el
bootstrap de bloques (que mato a las dos hipotesis hermanas: mkt_vol_168 del
banco y el funding extremo del item 4.2).

CUANDO CORRERLO, Y QUE ESPERAR. Las fechas estan preregistradas en
`HANDOFF_CIERRE.md`: 2026-10-19 (9 semanas, matar temprano), 2026-12-21 y
2027-12-13. Antes de las SEM_MIN semanas el script imprime los numeros pero NO
emite veredicto — dice TODAVIA NO ALCANZA. Mirar temprano esta bien; decidir
temprano es lo que fabrica el falso positivo, sobre todo cuando el numero sale
a favor. Ya paso una vez: el vistazo del 2026-08-27, anotado en el handoff.
"""
import argparse
import glob
import json
import os

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".cache")
FUNDING = os.path.join(os.path.dirname(HERE), "basis", ".funding_cache")
URL = "https://ecgdswroygkfckkaguxp.supabase.co"
KEY = os.environ.get("SUPABASE_KEY") or (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVjZ2Rzd3JveWdrZmNra2FndXhwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM1MTUyNzEsImV4cCI6MjA4OTA5MTI3MX0."
    "N_qJsJWTJaqRHpugzlnRTpoZI84mUoctt3RKmUshIrU")

AYUDA_KEY = r'''  La vigente esta en la memoria del proyecto. Cargarla sin imprimirla:

  $env:SUPABASE_KEY = (Select-String -Path "$env:USERPROFILE\.claude\projects\C--Users-asd-scancrypto-scanall\memory\reference-supabase-anon-key.md" -Pattern 'eyJ[A-Za-z0-9_.\-]+' -AllMatches).Matches[0].Value
'''

EXTENSION = ["EXPLOSION", "BREAKOUT"]
COSTO = 0.0040      # 0,09% fee perp ida+vuelta + 0,30% slippage asumido
SEM_MIN = 9         # el chequeo de "matar temprano" preregistrado para el 2026-10-19.
                    # El cuello NO son las alertas (entran ~130/semana) sino las
                    # semanas, que son la unidad independiente. Antes de eso el script
                    # muestra numeros pero NO emite veredicto: mirar temprano esta bien,
                    # decidir temprano es lo que fabrica el falso positivo.
RNG = np.random.default_rng(11)


def _pedir(desde):
    """Una pasada paginada sobre daytrader_outcomes."""
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}
    rows, off = [], 0
    while True:
        params = {"select": "*", "order": "alerted_at.asc",
                  "signal_type": f"in.({','.join(EXTENSION)})"}
        if desde:
            params["alerted_at"] = f"gte.{desde}"
        r = requests.get(f"{URL}/rest/v1/daytrader_outcomes",
                         headers={**h, "Range": f"{off}-{off+999}"},
                         params=params, timeout=60)
        if r.status_code in (401, 403):
            raise SystemExit(
                f"\nSupabase devolvio {r.status_code}: la anon key de fallback esta "
                f"vencida o rotada.\n{AYUDA_KEY}")
        r.raise_for_status()
        b = r.json()
        if not b:
            break
        rows.extend(b)
        if len(b) < 1000:
            break
        off += 1000
    return rows


def bajar(desde=None, offline=False):
    """daytrader_outcomes paginado, con cache INCREMENTAL.

    OJO — la version anterior devolvia el cache entero si el archivo existia, sin
    mirar la fecha. Para un forward test eso es veneno: el handoff dice "correr
    `evaluar.py` el 2026-10-19", y con `dt_all.json` escrito el 2026-08-17 eso
    habria reimpreso los datos IN-SAMPLE —los mismos con los que se construyo la
    hipotesis— con cara de out-of-sample, y encima con las cuatro compuertas en OK.

    Ahora el cache es un piso, no una respuesta: se pide siempre el tramo nuevo, y
    ademas se re-pide la ultima semana porque las filas recien alertadas entran con
    `price_4h`/`price_24h` en NULL y se completan despues (`outcomes_complete`).
    """
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"dt_{desde or 'all'}.json")
    previas = json.load(open(p, encoding="utf-8")) if os.path.exists(p) else []
    if offline:
        return pd.DataFrame(previas)

    corte = desde
    if previas:
        tope = max(r["alerted_at"] for r in previas)
        remojo = pd.Timestamp(tope) - pd.Timedelta(days=8)   # outcomes aun madurando
        corte = max(remojo.isoformat(), desde or "") or None

    filas = {r["id"]: r for r in previas}
    filas.update({r["id"]: r for r in _pedir(corte)})
    rows = sorted(filas.values(), key=lambda r: r["alerted_at"])
    json.dump(rows, open(p, "w", encoding="utf-8"))
    return pd.DataFrame(rows)


def con_perp():
    """Simbolos con perpetuo USDT. Sin perp no hay short: no es opcional."""
    s = set()
    for f in glob.glob(os.path.join(FUNDING, "*.csv")):
        n = os.path.basename(f).split("_")[0]
        s.add(n)
        for pre in ("1000000", "1000"):
            if n.startswith(pre):
                s.add(n[len(pre):])
    return s


def p_semanas(d, col="f", reps=8000):
    """IC y p-valor con LA SEMANA como unidad independiente.

    OJO — esto reemplaza a una version anterior que remuestreaba bloques de
    semanas pero POOLEABA las alertas de cada bloque. Eso hacia dos cosas mal:
    las semanas con mas alertas pesaban mas (van de 66 a 199 por semana), y con
    8 semanas hay 7 bloques distintos posibles, asi que remuestrear de ahi
    SUBESTIMA la variabilidad. Daba IC [+0,17, +2,32] cuando el correcto es
    [-0,50, +3,55], que cruza cero.

    Si la unidad independiente es la semana — que es la premisa entera de
    remuestrear por bloques — cada semana pesa igual y se remuestrean semanas
    ENTERAS. El desvio entre semanas (2,92pp) es casi el doble de la media
    (1,52pp): esa es la verdadera relacion senal-ruido de esta estrategia.
    """
    wm = np.array([g[col].mean() for _, g in d.groupby("week", sort=True)])
    k = len(wm)
    if k < 4:
        return 1.0, (np.nan, np.nan)
    m = np.array([RNG.choice(wm, k, replace=True).mean() for _ in range(reps)])
    return float((m <= 0).mean()), tuple(np.percentile(m, [2.5, 97.5]))


def semanas_necesarias(d, col="f", potencia=0.80, factor=1.0):
    """Cuantas semanas NUEVAS hacen falta para un forward test con potencia.

    `factor` escala el efecto esperado: 0.5 = suponer que el real es la mitad
    del medido, que es lo normal (la primera medicion siempre exagera).
    """
    wm = np.array([g[col].mean() for _, g in d.groupby("week", sort=True)])
    if len(wm) < 2:
        return float("nan")          # con una semana no hay desvio que estimar
    mu, sd = wm.mean() * factor, wm.std(ddof=1)
    if mu <= 0 or not np.isfinite(sd):
        return float("inf")
    z = {0.50: 0.0, 0.80: 0.84, 0.90: 1.28}[potencia]
    return int(np.ceil(((1.96 + z) * sd / mu) ** 2))


def evaluar(df, horizonte="24h", fill="price_15m", solo_perp=True, costo=COSTO):
    d = df.copy()
    d["alerted_at"] = pd.to_datetime(d["alerted_at"], utc=True, format="mixed")
    d["week"] = d["alerted_at"].dt.tz_localize(None).dt.to_period("W")
    if solo_perp:
        d = d[d.symbol.isin(con_perp())]
    # fadear = shortear: se gana cuando el precio BAJA desde el fill
    d["f"] = -(d[f"price_{horizonte}"] / d[fill] - 1) - costo
    d = d.dropna(subset=["f"])

    ap = d.groupby("symbol").f.sum().sort_values()
    sin3 = d[~d.symbol.isin(ap.tail(3).index)].f
    sin_peor = d[d.symbol != ap.index[0]].f
    w = d.groupby("week").f.agg(["size", "mean"])
    w = w[w["size"] >= 20]
    p, ic = p_semanas(d)

    g = {
        "(a) media > 0": (d.f.mean() > 0, f"{100*d.f.mean():+.3f}%"),
        "(b) sin top-3": (sin3.mean() > 0, f"{100*sin3.mean():+.3f}%"),
        "(c) >=75% semanas": ((w["mean"] > 0).sum() >= np.ceil(0.75 * len(w)),
                              f"{(w['mean']>0).sum()}/{len(w)}"),
        "(d) sin el peor simbolo": (sin_peor.mean() > 0, f"{100*sin_peor.mean():+.3f}%"),
        "(e) semanas: IC no cruza 0": (ic[0] > 0,
                                       f"p={p:.4f} IC[{100*ic[0]:+.2f},{100*ic[1]:+.2f}]"),
    }
    print(f"\n--- {horizonte} | fill={fill} | {'solo perps' if solo_perp else 'todas'} "
          f"| costo {100*costo:.2f}% | n={len(d)} | {len(w)} semanas ---")
    for k, (ok, v) in g.items():
        print(f"  {k:28s} {v:>26s}   {'OK' if ok else 'FALLA'}")

    # Tres estados, no dos. Con menos de SEM_MIN semanas el bootstrap ni siquiera
    # corre (p_semanas devuelve nan) y (e) sale FALLA por falta de datos, no por el
    # resultado: imprimir "LA REGLA DISPARA" ahi seria leer una muerte donde solo hay
    # una medicion que todavia no existe. Es el mismo error que `radar/medir.py` ya
    # evita con su "TODAVIA NO ALCANZA". Un "no se pudo medir" NO es "no esta".
    if len(w) < SEM_MIN:
        print(f"  ---> TODAVIA NO ALCANZA: {len(w)} de {SEM_MIN} semanas preregistradas.")
        print("       Las medias de arriba son informativas y NO deciden nada.")
        # con menos de 4 semanas el desvio entre semanas es puro ruido: la cuenta de
        # potencia da cualquier cosa (con 2 semanas puede decir "faltan 2"). No se imprime.
        faltan = semanas_necesarias(d, potencia=0.80, factor=0.5) if len(w) >= 4 else None
        if faltan is not None and np.isfinite(faltan):
            print(f"       Con este ruido semanal harian falta ~{faltan} semanas para "
                  f"un efecto la mitad del medido.")
        return None
    todo = all(ok for ok, _ in g.values())
    print(f"  ---> {'SOBREVIVE' if todo else 'LA REGLA DISPARA'}")
    return todo


if __name__ == "__main__":
    ap_ = argparse.ArgumentParser()
    ap_.add_argument("--desde", default=None, help="ISO date; solo alertas posteriores")
    ap_.add_argument("--costo", type=float, default=COSTO)
    a = ap_.parse_args()

    df = bajar(a.desde)
    print("=" * 78)
    print("4.7 — FADEAR LAS SENALES DE EXTENSION")
    print("=" * 78)
    print(f"{len(df)} alertas  |  {df.signal_type.value_counts().to_dict()}")
    if len(df):
        print(f"ventana {df.alerted_at.min()[:10]} -> {df.alerted_at.max()[:10]}")
    print("\nCONDICION REALISTA (fill 15m despues de la alerta, solo simbolos con perp):")
    for h in ("4h", "24h"):
        evaluar(df, h, "price_15m", True, a.costo)
    print("\nreferencia optimista (fill al precio de la alerta, todos los simbolos):")
    for h in ("4h", "24h"):
        evaluar(df, h, "entry_price", False, a.costo)
    print("\n" + "=" * 78)
    print("RECORDATORIO: la ventana que construyo la hipotesis son 51 dias de UN SOLO")
    print("regimen bear. Lo que falta no es otro test estadistico — es un tramo")
    print(f"ALCISTA y {SEM_MIN} semanas limpias. Fechas preregistradas: 2026-10-19")
    print("(matar temprano) · 2026-12-21 · 2027-12-13. Nada de capital hasta entonces.")
    print("=" * 78)
