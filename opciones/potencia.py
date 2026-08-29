"""
CORRIDA 8 — compuerta (C): se puede MEDIR la prima en alts?

Regla de parada, textual de banco/PREREGISTRO_OPCIONES.md §6, escrita antes de
calcular un solo MDE:

    Para cada subyacente candidato: si el MDE sobre el retorno neto ANUALIZADO de la
    straddle ATM mensual no solapada, a 80% de potencia y alfa 0,05 a dos colas, es
    > 10%/ano, ese subyacente se declara "no se pudo medir". Si quedan menos de 3
    subyacentes medibles, la direccion 2.1 se cierra por potencia.

    MDE = 2,8 x sigma_mensual / sqrt(n_meses) x 12

Este script NO mira el signo ni el tamano de la prima. Calcula n y sigma. El numero
de la prima se mira despues, y solo para quien sobreviva (C).

Fuentes:
  - implicita: indice a 30d de Bybit. GOTCHA: exige quoteCoin=USDT (sin el devuelve
    SUCCESS con lista vacia, que parece "no hay datos" y no lo es) y ventanas <= 30d.
  - realizada: cierres diarios de Binance spot, vol de los 30 dias SIGUIENTES, que es
    contra lo que se cobra de verdad (mismo estimador que iv_rv.py).

    $env:PYTHONIOENCODING = "utf-8"
    py -3.13 -u potencia.py
"""
import os
import sys
import time

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".dvol_cache")

DIAS = 30                 # horizonte de la straddle
MDE_MAX = 10.0            # %/ano — el umbral preregistrado
MIN_SUBS = 3              # subyacentes medibles que hacen falta
Z = 2.8                   # 1,96 (alfa 0,05 dos colas) + 0,84 (80% de potencia)

# los candidatos que pasaron (A) y (B), mas BTC/ETH como calibracion conocida
CAND = [("SOL", "SOLUSDT"), ("XRP", "XRPUSDT"), ("HYPE", "HYPEUSDT")]
CALIB = [("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")]

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0"})


def iv_bybit(moneda):
    """Indice de vol implicita a 30d, horario, paginado hacia atras en ventanas de 25d."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"bybitiv_{moneda}.csv")
    if os.path.exists(p):
        return pd.read_csv(p, parse_dates=["fecha"]).set_index("fecha")["iv"]

    filas, fin, vacias = [], pd.Timestamp.utcnow().tz_convert("UTC"), 0
    for _ in range(80):                       # 80 x 25d = ~5,5 anos
        ini = fin - pd.Timedelta(days=25)
        try:
            r = S.get("https://api.bybit.com/v5/market/historical-volatility",
                      params={"category": "option", "baseCoin": moneda,
                              "quoteCoin": "USDT", "period": "30",
                              "startTime": int(ini.timestamp() * 1000),
                              "endTime": int(fin.timestamp() * 1000)}, timeout=30).json()
        except Exception:
            r = {}
        res = r.get("result") or []
        if res:
            filas.extend(res)
            vacias = 0
        else:
            vacias += 1
            if vacias >= 2:
                break
        fin = ini
        time.sleep(0.12)

    if not filas:
        return pd.Series(dtype=float, name="iv")
    d = pd.DataFrame(filas).drop_duplicates("time")
    d["fecha"] = pd.to_datetime(d["time"].astype("int64"), unit="ms",
                                utc=True).dt.tz_localize(None).dt.normalize()
    # el indice viene en fraccion (0,3736 = 37,36%); a % para igualar a iv_rv.py
    s = d.assign(iv=d["value"].astype(float) * 100).groupby("fecha")["iv"].mean()
    s.to_frame().reset_index().to_csv(p, index=False)
    return s


def cierres(sym):
    """Cierres diarios de Binance spot, cacheados."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"px_{sym}.csv")
    if os.path.exists(p):
        return pd.read_csv(p, parse_dates=["fecha"]).set_index("fecha")["c"]
    filas = []
    cur = int(pd.Timestamp("2024-01-01", tz="UTC").timestamp() * 1000)
    fin = int(pd.Timestamp.utcnow().timestamp() * 1000)
    while cur < fin:
        r = S.get("https://api.binance.com/api/v3/klines",
                  params={"symbol": sym, "interval": "1d", "startTime": cur,
                          "endTime": fin, "limit": 1000}, timeout=30)
        if r.status_code != 200:
            print(f"    ! binance {sym}: {r.status_code}")
            break
        d = r.json()
        if not d:
            break
        filas.extend(d)
        nuevo = int(d[-1][0])
        if len(d) < 1000 or nuevo <= cur:
            break
        cur = nuevo + 1
    if not filas:
        return pd.Series(dtype=float, name="c")
    s = pd.DataFrame([{"t": int(x[0]), "c": float(x[4])} for x in filas]).drop_duplicates("t")
    s["fecha"] = pd.to_datetime(s["t"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
    s = s.groupby("fecha")["c"].last()
    s.to_frame().reset_index().to_csv(p, index=False)
    return s


def potencia(moneda, sym):
    iv = iv_bybit(moneda)
    px = cierres(sym)
    if iv.empty or px.empty:
        return {"moneda": moneda, "n": 0, "nota": "sin serie"}

    # realizada de los 30 dias SIGUIENTES: contra eso se cobra
    rv = np.log(px).diff().rolling(DIAS).std().shift(-DIAS) * np.sqrt(365) * 100
    d = pd.DataFrame({"iv": iv, "rv": rv}).dropna()
    if len(d) < DIAS:
        return {"moneda": moneda, "n": 0, "nota": "solapamiento insuficiente",
                "dias": len(d)}

    # la unidad de observacion son MESES no solapados, no dias
    m = d.iloc[::DIAS]
    n = len(m)

    # P&L de vender una straddle ATM 30d, en % del spot (misma formula que iv_rv.py):
    # la prima ATM vale 0,7979 x sigma x sqrt(T) x S, asi que la diferencia de vol se
    # traduce directo.
    k = 0.7979 * np.sqrt(DIAS / 365)
    sig_rv = k * m.rv.std()          # el sigma preregistrado (dispersion de la realizada)
    sig_dif = k * (m.iv - m.rv).std()  # el mas ajustado: iv y rv estan correlacionadas

    mde_rv = Z * sig_rv / np.sqrt(n) * 12 if n else np.inf
    mde_dif = Z * sig_dif / np.sqrt(n) * 12 if n else np.inf
    return {"moneda": moneda, "n": n, "dias": len(d),
            "desde": d.index[0], "hasta": d.index[-1],
            "sig_rv": sig_rv, "sig_dif": sig_dif,
            "mde_rv": mde_rv, "mde_dif": mde_dif}


def main():
    print("=" * 92)
    print("CORRIDA 8 — COMPUERTA (C): POTENCIA. Se puede medir la prima en alts?")
    print("=" * 92)
    print(f"regla preregistrada: MDE del retorno neto anualizado <= {MDE_MAX:.0f}%/ano,")
    print(f"                     y >= {MIN_SUBS} subyacentes medibles, o la direccion se cierra.")
    print("                     MDE = 2,8 x sigma_mensual / sqrt(n_meses) x 12")
    print("\nunidad de observacion: MESES no solapados. La serie horaria de Bybit tiene")
    print("~720 puntos por mes y ninguno es una observacion independiente.\n")

    filas = []
    for moneda, sym in CALIB + CAND:
        print(f"  bajando {moneda}...", flush=True)
        r = potencia(moneda, sym)
        r["rol"] = "calibracion" if (moneda, sym) in CALIB else "candidato"
        filas.append(r)

    print(f"\n{'='*92}\nn Y MDE (el signo y el tamano de la prima NO se miran aca)\n{'='*92}")
    print(f"{'sub':<7}{'rol':<14}{'ventana':<26}{'dias':>6}{'MESES':>7}"
          f"{'sigma/mes':>11}{'MDE %/ano':>12}")
    for r in filas:
        if not r.get("n"):
            print(f"{r['moneda']:<7}{r['rol']:<14}{r.get('nota','-'):<26}"
                  f"{r.get('dias',0):>6}{0:>7}{'-':>11}{'inf':>12}")
            continue
        vent = f"{r['desde']:%Y-%m} -> {r['hasta']:%Y-%m}"
        # se usa el MDE MAS AJUSTADO de los dos: si algo cierra, que no cierre por
        # un supuesto pesimista
        mde = min(r["mde_rv"], r["mde_dif"])
        print(f"{r['moneda']:<7}{r['rol']:<14}{vent:<26}{r['dias']:>6}{r['n']:>7}"
              f"{min(r['sig_rv'], r['sig_dif']):>10.2f}%{mde:>11.1f}%")

    print("\n  (sigma/mes en % del spot; se reporta el MDE mas ajustado de los dos"
          " estimadores,\n   dispersion de la realizada y dispersion de iv-rv)")

    cand = [r for r in filas if r["rol"] == "candidato"]
    print(f"\n{'='*92}\nVEREDICTO DE (C)\n{'='*92}")
    ok = []
    for r in cand:
        if not r.get("n"):
            print(f"  {r['moneda']:<6} n=0 meses          -> NO SE PUDO MEDIR")
            continue
        mde = min(r["mde_rv"], r["mde_dif"])
        pasa = mde <= MDE_MAX
        if pasa:
            ok.append(r["moneda"])
        print(f"  {r['moneda']:<6} n={r['n']:>3} meses  MDE {mde:>6.1f}%/ano  -> "
              f"{'medible' if pasa else 'NO SE PUDO MEDIR'}")

    print(f"\n  subyacentes medibles: {len(ok)}  {sorted(ok)}   (hacen falta {MIN_SUBS})")

    # --- cuantos meses HARIAN FALTA, y la calibracion contra los 5,3 anos de DVOL ---
    print(f"\n{'-'*92}")
    print("CUANTA HISTORIA HARIA FALTA para llegar al MDE preregistrado de "
          f"{MDE_MAX:.0f}%/ano:      n = (2,8 x sigma_mensual / (MDE/12))^2")
    for r in filas:
        if not r.get("n"):
            continue
        sig = min(r["sig_rv"], r["sig_dif"])
        nec = (Z * sig / (MDE_MAX / 12)) ** 2
        print(f"  {r['moneda']:<6} sigma {sig:5.2f}%/mes  ->  {nec:6.0f} meses "
              f"= {nec/12:5.1f} anos   (hay {r['n']} meses = {r['n']/12:.1f})")

    print(f"\n{'-'*92}")
    print("CALIBRACION contra lo que SI concluyo: DVOL de Deribit, BTC/ETH, 5,3 anos.")
    print("Si con la muestra larga el MDE tampoco baja del umbral, el problema no es")
    print("que las alts sean jovenes: es el estimador.")
    try:
        from iv_rv import dvol, cierres as cierres_dvol
        for mon, sym in (("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")):
            iv = dvol(mon)
            px = cierres_dvol(sym)
            rv = np.log(px).diff().rolling(DIAS).std().shift(-DIAS) * np.sqrt(365) * 100
            d = pd.DataFrame({"iv": iv, "rv": rv}).dropna()
            m = d.iloc[::DIAS]
            k = 0.7979 * np.sqrt(DIAS / 365)
            sig = min(k * m.rv.std(), k * (m.iv - m.rv).std())
            mde = Z * sig / np.sqrt(len(m)) * 12
            nec = (Z * sig / (MDE_MAX / 12)) ** 2
            print(f"  {mon:<6} {d.index[0]:%Y-%m} -> {d.index[-1]:%Y-%m}  "
                  f"n={len(m):>3} meses  sigma {sig:5.2f}%/mes  MDE {mde:5.1f}%/ano"
                  f"   (harian falta {nec/12:.1f} anos)")
    except Exception as e:
        print(f"  ! no se pudo calibrar: {type(e).__name__} {e}")


    # --- la unica salida que le queda al estimador: poolear entre subyacentes ---
    print(f"\n{'-'*92}")
    print("LA SALIDA QUE LE QUEDA: poolear varios subyacentes en un solo estimador.")
    print("Gana potencia solo si sus errores son independientes. n_efectivo = k / (1+(k-1)*rho)")
    ser = {}
    for mon, sym in CALIB + CAND:
        iv, px = iv_bybit(mon), cierres(sym)
        if iv.empty or px.empty:
            continue
        rv = np.log(px).diff().rolling(DIAS).std().shift(-DIAS) * np.sqrt(365) * 100
        d = pd.DataFrame({"iv": iv, "rv": rv}).dropna()
        if len(d) < DIAS:
            continue
        # OJO: aca se muestrea por MES CALENDARIO, no con iloc[::30]. Con el paso
        # posicional cada moneda arranca en un dia distinto y la interseccion de
        # fechas queda vacia; el bloque entero se salteaba en silencio.
        m = d.resample("MS").first().dropna()
        ser[mon] = (0.7979 * np.sqrt(DIAS / 365)) * (m.iv - m.rv)
    P = pd.DataFrame(ser).dropna()
    if len(P.columns) >= 2 and len(P) >= 5:
        C = P.corr()
        print(f"\n  correlacion del P&L mensual de la straddle ({len(P)} meses en comun):")
        print("        " + "".join(f"{c:>8}" for c in C.columns))
        for i, row in C.iterrows():
            print(f"  {i:<6}" + "".join(f"{row[c]:>8.2f}" for c in C.columns))
        off = C.values[np.triu_indices(len(C), 1)]
        rho = float(np.mean(off))
        k = len(C)
        nef = k / (1 + (k - 1) * rho)
        print(f"\n  rho medio entre pares = {rho:+.2f}   con k={k} subyacentes")
        print(f"  n efectivo = {nef:.2f} subyacentes independientes, no {k}")
        print(f"  -> la barra de error se angosta {np.sqrt(nef):.2f}x, no {np.sqrt(k):.2f}x")
        base = [r for r in filas if r["moneda"] == "SOL" and r.get("n")]
        if base:
            mde = min(base[0]["mde_rv"], base[0]["mde_dif"]) / np.sqrt(nef)
            print(f"  -> MDE pooleado sobre la ventana de SOL: {mde:.1f}%/ano "
                  f"(umbral {MDE_MAX:.0f}%)")
            print(f"     {'sigue sin alcanzar.' if mde > MDE_MAX else 'ALCANZA: revisar.'}")
        print("\n  La volatilidad de cripto es basicamente UN factor. Sumar monedas")
        print("  agrega nombres, no informacion independiente.")

    if len(ok) >= MIN_SUBS:
        print("\n  (C) PASA. Recien ahora se puede mirar la prima.")
    else:
        print("\n  (C) FALLA. La direccion 2.1 se CIERRA POR POTENCIA.")
        print("  El instrumento existe y se puede cruzar —(A) y (B) pasaron—, pero la")
        print("  historia publica de vol implicita para alts no alcanza para distinguir")
        print("  un edge real de ruido. No se estima la prima: seria un numero sin")
        print("  barra de error util, que es exactamente lo que este repo no hace.")
    print("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
