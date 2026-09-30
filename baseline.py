"""
baseline_random.py - El baseline que falta.

Pregunta: si en vez de tus alertas hubiera comprado monedas al azar
en esos mismos momentos, me habria ido mejor o peor?

Descarga klines de 15m de N simbolos random para todo el periodo del dump
(una request por simbolo, no una por alerta) y compara.

ESTE SCRIPT HAY QUE CORRERLO EN TU MAQUINA - necesita acceso a api.binance.com.

Uso:  python baseline_random.py outcomes_dump.json --n-random 80
"""
import json, sys, time, random, argparse, os
from datetime import datetime, timezone
from collections import defaultdict

import requests

BASE = "https://api.binance.com"
CACHE = ".baseline_cache"
HORIZONTES = {'15m': 1, '1h': 4, '4h': 16, '8h': 32, '24h': 96}  # en velas de 15m
COSTO = 0.002


def simbolos_usdt():
    """Todos los pares USDT spot que estan operando."""
    r = requests.get(BASE + "/api/v3/exchangeInfo", timeout=30)
    r.raise_for_status()
    out = []
    for s in r.json()['symbols']:
        if (s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING'
                and not s['symbol'].endswith(('UPUSDT', 'DOWNUSDT'))):
            out.append(s['symbol'])
    return out


def klines(symbol, ini_ms, fin_ms):
    """Velas de 15m entre ini y fin, con cache en disco."""
    os.makedirs(CACHE, exist_ok=True)
    ruta = f"{CACHE}/{symbol}_{ini_ms}_{fin_ms}.json"
    if os.path.exists(ruta):
        return json.load(open(ruta))

    todas = []
    desde = ini_ms
    while desde < fin_ms:
        r = requests.get(BASE + "/api/v3/klines", timeout=30, params={
            'symbol': symbol, 'interval': '15m',
            'startTime': desde, 'endTime': fin_ms, 'limit': 1000})
        if r.status_code == 429:            # rate limit
            time.sleep(10)
            continue
        r.raise_for_status()
        lote = r.json()
        if not lote:
            break
        todas += lote
        desde = lote[-1][0] + 1
        if len(lote) < 1000:
            break
        time.sleep(0.15)

    json.dump(todas, open(ruta, 'w'))
    return todas


def indexar(velas):
    """{open_time_ms: open_price} para poder buscar por timestamp."""
    return {int(v[0]): float(v[1]) for v in velas}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('dump')
    ap.add_argument('--n-random', type=int, default=80,
                    help='cuantos simbolos random descargar')
    ap.add_argument('--semilla', type=int, default=0)
    args = ap.parse_args()

    d = json.load(open(args.dump))
    alertas = d['main'] if isinstance(d, dict) else d
    for a in alertas:
        a['t'] = datetime.fromisoformat(a['alerted_at'])

    ini = min(a['t'] for a in alertas)
    fin = max(a['t'] for a in alertas)
    # margen de 24h al final para que existan las velas del horizonte largo
    ini_ms = int(ini.timestamp() * 1000)
    fin_ms = int(fin.timestamp() * 1000) + 25 * 3600 * 1000
    print(f"Periodo: {ini} -> {fin}")

    todos = simbolos_usdt()
    print(f"Pares USDT operando: {len(todos)}")
    rnd = random.Random(args.semilla)
    muestra = rnd.sample(todos, min(args.n_random, len(todos)))

    datos = {}
    for i, sym in enumerate(muestra, 1):
        try:
            datos[sym] = indexar(klines(sym, ini_ms, fin_ms))
            print(f"  [{i}/{len(muestra)}] {sym} ok", end='\r')
        except Exception as e:
            print(f"  [{i}/{len(muestra)}] {sym} FALLO: {e}")
    print(f"\nDescargados {len(datos)} simbolos.\n")

    # para cada momento en que hubo alerta, tomamos TODOS los symbols random
    # como si hubieran sido alertas -> esa es la distribucion nula
    momentos = sorted(set(int(a['t'].timestamp() // 900 * 900 * 1000) for a in alertas))
    print(f"Momentos distintos con alerta: {len(momentos)}")

    nulos = defaultdict(list)
    for ms in momentos:
        for sym, idx in datos.items():
            e = idx.get(ms)
            if not e:
                continue
            for h, k in HORIZONTES.items():
                p = idx.get(ms + k * 900_000)
                if p:
                    nulos[h].append(p / e - 1)

    reales = defaultdict(list)
    for a in alertas:
        e = a['entry_price']
        for h in HORIZONTES:
            p = a.get('price_' + h)
            if p and e:
                reales[h].append(p / e - 1)

    def stats(v):
        v = sorted(v)
        n = len(v)
        return (n, sum(v) / n, v[n // 2], sum(1 for x in v if x > 0) / n)

    print("\n=== TUS ALERTAS vs COMPRAR AL AZAR ===")
    print(f"{'horiz':>6} | {'n':>5} {'media':>8} {'%pos':>6} | "
          f"{'n':>6} {'media':>8} {'%pos':>6} | {'ventaja':>9}")
    print("-" * 72)
    for h in HORIZONTES:
        if not reales[h] or not nulos[h]:
            continue
        na, ma, _, pa = stats(reales[h])
        nn, mn, _, pn = stats(nulos[h])
        print(f"{h:>6} | {na:5d} {100*ma:+7.2f}% {100*pa:5.1f}% | "
              f"{nn:6d} {100*mn:+7.2f}% {100*pn:5.1f}% | {100*(ma-mn):+8.2f}%")

    print("\nLa columna 'ventaja' es lo unico que importa.")
    print(f"Tiene que ser mayor a {100*COSTO:.1f}% (costos) para que el screener sirva.")
    print("Si es negativa o cercana a cero, no hay edge y no hay nada que tunear.")


if __name__ == '__main__':
    main()