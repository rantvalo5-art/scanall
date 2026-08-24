"""
TECHO CONDICIONAL — ¿hay ALGUNA informacion aprovechable en el feed de alertas?

El repo lleva ~450 hipotesis probadas de a una, casi todas features de precio/volumen,
todas muertas con la misma firma (intercambian mediana contra cola y dejan la media en
cero). Probar la 451 no cambia nada. Esto pregunta otra cosa:

    usando SOLO lo que se sabe en el instante de la alerta, ¿cuanto del resultado
    se puede predecir?

Es la contracara del techo oraculo. Aquel midio el techo de la SELECCION (con vision
perfecta el feed daba x966, y el bot ya alerta sobre el 83% de lo que elegiria el
oraculo). Esto mide el techo de lo CONDICIONAL. Un negativo aca no dice "esta feature
no": dice **ni esta ni ninguna combinacion de las que hay**.

## Objetivo

`margen = retorno - dardo pareado` (mismo simbolo, horas al azar de +-21d saltando +-7d).
El dardo descuenta de una la seleccion de moneda, el sesgo de universo y la caida general
del mercado; lo que queda es lo que el bot elige: el MOMENTO.

## Lo que se le da al modelo (todo conocido al alertar, sin lookahead)

- de la tabla: score, tipo de senal, timeframe, bucket, candle_status, obv_slope,
  cvd_ratio, recent_long_ok, funding_rate, log10(precio) y la EXTENSION
  (entry_price / ref_price - 1), que es cuanto por encima del nivel disparo
- del propio feed: cuantas alertas previas hubo de ese simbolo en 7d, horas desde la
  ultima, si es la primera, e intensidad global del feed en las 24h previas
- de las klines (barra i = ultima CERRADA, offset -1): los 8 atributos preregistrados
  de `pileta.atributos` (liquidez, ATR%, drawdown 90d, ret 30d, edad, beta y corr contra
  BTC, precio) mas retornos previos de 24h/72h/168h, posicion dentro del rango de 60
  barras, distancia al maximo reciente y ratio de volumen
- de BTC: retorno previo 24h/168h y volatilidad realizada 168h (el regimen)
- de los dumps de futuros: tt_pos, oi, ls_cuentas, tt_cuentas, taker y sus z de 14d.
  Van de CONTROL NEGATIVO: `tt_pos` ya se tumbo (project-swing-ttpos-evitar), asi que
  si el modelo se apoya ahi es senal de que esta ajustando ruido.

## Los dos modelos

Ridge (lineal, regularizado) y boosting de arboles depth-2 (captura interacciones), los
dos en numpy — el stack del repo no tiene sklearn y no se le agrega una dependencia por
un experimento. El lineal solo daria una COTA INFERIOR del techo; el no lineal es el que
hace la pregunta de verdad.

## Honestidad

- Particion TEMPORAL por semanas: se entrena en las primeras y se mide en las ultimas.
  Los hiperparametros salen de una validacion interna DENTRO del tramo de entrenamiento.
- **Null de permutacion**: se corre el pipeline entero N veces con el objetivo desplazado
  circularmente (preserva la autocorrelacion del retorno y rompe el vinculo con las
  features). Eso le pone precio a la busqueda: dice cuanto produce este mismo pipeline
  a partir de puro ruido.
- Bootstrap por SEMANA para el IC, y las dos concentraciones.

## REGLA DE PARADA — escrita ANTES de correr nada

> Hay informacion condicional aprovechable si, en las semanas retenidas, el decil
> superior del modelo tiene margen POSITIVO con IC95 (bootstrap por semana) que no toca
> el cero, **Y** ese margen supera el percentil 95 del null de permutacion. Las dos
> cosas, para al menos uno de los dos modelos.
>
> Si no: **no hay estrategia condicional en este feed**, y eso vale tambien para las
> combinaciones, no solo para las features de a una.

    py -3.13 techo_condicional.py --horizonte-h 168
    py -3.13 techo_condicional.py --horizonte-h 24 --cross
"""
import argparse
import json
import random
import time
from pathlib import Path

import numpy as np
import pandas as pd

import dip_previo as D
import fase0_plan as F
import pileta as P

CSV_SWING = F.HERE.parent / "archivo_outcomes" / "screener_outcomes.csv"
CSV_DT = F.HERE.parent / "archivo_outcomes" / "daytrader_outcomes.csv"
CACHE = F.HERE / ".techo_cache"
CACHE.mkdir(exist_ok=True)


# ════════════════════════════════════════════════════════════════════════════
# Modelos, en numpy
# ════════════════════════════════════════════════════════════════════════════
def ridge(Xtr, ytr, Xte, alpha):
    mu, sd = Xtr.mean(0), Xtr.std(0)
    sd = np.where(sd > 1e-12, sd, 1.0)
    A = (Xtr - mu) / sd
    B = (Xte - mu) / sd
    ym = ytr.mean()
    G = A.T @ A + alpha * np.eye(A.shape[1])
    w = np.linalg.solve(G, A.T @ (ytr - ym))
    return B @ w + ym


class Boost:
    """Boosting de arboles depth-2 sobre features binneadas (histogramas).

    Perdida cuadratica. Con ~2.000 filas y ~40 features un depth mayor solo ajusta
    ruido; depth-2 alcanza para capturar interacciones de a pares, que es lo que
    un modelo lineal no puede y por lo que vale correrlo.
    """

    def __init__(self, n=200, lr=0.06, bins=12, min_hoja=50, seed=0):
        self.n, self.lr, self.bins, self.min_hoja = n, lr, bins, min_hoja
        self.rng = np.random.default_rng(seed)

    def _binner(self, X):
        self.cortes = []
        for j in range(X.shape[1]):
            q = np.unique(np.quantile(X[:, j], np.linspace(0, 1, self.bins + 1)[1:-1]))
            self.cortes.append(q)

    def _bin(self, X):
        return np.stack([np.searchsorted(self.cortes[j], X[:, j]).astype(np.int16)
                         for j in range(X.shape[1])], axis=1)

    def _mejor_corte(self, Xb, g, idx):
        """Mejor (feature, umbral) por reduccion de suma de cuadrados."""
        if len(idx) < 2 * self.min_hoja:
            return None
        gg = g[idx]
        tot_s, tot_n = gg.sum(), len(gg)
        mejor = None
        for j in range(Xb.shape[1]):
            b = Xb[idx, j]
            s = np.bincount(b, weights=gg, minlength=self.bins)
            c = np.bincount(b, minlength=self.bins).astype(float)
            cs, cc = np.cumsum(s)[:-1], np.cumsum(c)[:-1]
            ok = (cc >= self.min_hoja) & ((tot_n - cc) >= self.min_hoja)
            if not ok.any():
                continue
            gan = np.where(ok, cs ** 2 / np.maximum(cc, 1) +
                           (tot_s - cs) ** 2 / np.maximum(tot_n - cc, 1), -np.inf)
            k = int(np.argmax(gan))
            if mejor is None or gan[k] > mejor[0]:
                mejor = (float(gan[k]), j, k)
        return mejor

    def fit(self, X, y, Xval=None, yval=None):
        self._binner(X)
        Xb = self._bin(X)
        self.base = float(y.mean())
        pred = np.full(len(y), self.base)
        pv = np.full(len(Xval), self.base) if Xval is not None else None
        Xvb = self._bin(Xval) if Xval is not None else None
        self.arboles = []
        mejor_val, mejor_n = np.inf, 0
        for t in range(self.n):
            g = y - pred
            arbol = self._arbol(Xb, g)
            if arbol is None:
                break
            self.arboles.append(arbol)
            pred += self.lr * self._aplicar(arbol, Xb)
            if pv is not None:
                pv += self.lr * self._aplicar(arbol, Xvb)
                e = float(np.mean((yval - pv) ** 2))
                if e < mejor_val:
                    mejor_val, mejor_n = e, len(self.arboles)
        if pv is not None:
            self.arboles = self.arboles[:max(mejor_n, 1)]
        return self

    def _arbol(self, Xb, g):
        n = len(g)
        raiz = self._mejor_corte(Xb, g, np.arange(n))
        if raiz is None:
            return None
        _, j0, k0 = raiz
        izq = np.where(Xb[:, j0] <= k0)[0]
        der = np.where(Xb[:, j0] > k0)[0]
        hijos = []
        for idx in (izq, der):
            c = self._mejor_corte(Xb, g, idx)
            if c is None:
                hijos.append(("hoja", float(g[idx].mean()) if len(idx) else 0.0))
            else:
                _, j, k = c
                a = idx[Xb[idx, j] <= k]
                b = idx[Xb[idx, j] > k]
                hijos.append(("corte", j, k,
                              float(g[a].mean()) if len(a) else 0.0,
                              float(g[b].mean()) if len(b) else 0.0))
        return (j0, k0, hijos[0], hijos[1])

    @staticmethod
    def _aplicar(arbol, Xb):
        j0, k0, h_izq, h_der = arbol
        out = np.zeros(len(Xb))
        m = Xb[:, j0] <= k0
        for mask, h in ((m, h_izq), (~m, h_der)):
            if h[0] == "hoja":
                out[mask] = h[1]
            else:
                _, j, k, va, vb = h
                sub = Xb[:, j] <= k
                out[mask & sub] = va
                out[mask & ~sub] = vb
        return out

    def predict(self, X):
        Xb = self._bin(X)
        p = np.full(len(X), self.base)
        for a in self.arboles:
            p += self.lr * self._aplicar(a, Xb)
        return p


# ════════════════════════════════════════════════════════════════════════════
# Datos: alertas -> objetivo (margen contra dardo) + features
# ════════════════════════════════════════════════════════════════════════════
def _num(s):
    return pd.to_numeric(s, errors="coerce")


def cargar(csv, H, darts, seed, senales=None, tag="swing"):
    p = CACHE / f"{tag}_h{H}_d{darts}_s{seed}.pkl"
    if p.exists():
        try:
            return pd.read_pickle(p)
        except Exception:
            pass
    df = pd.read_csv(csv)
    if senales:
        df = df[df.signal_type.isin(senales)]
    df = df.copy()
    df["ts"] = pd.to_datetime(df.alerted_at, utc=True, format="mixed")
    df = df.sort_values("ts").reset_index(drop=True)
    df["ts_ms"] = df.ts.astype("int64") // 10**6
    df["week"] = df.ts.dt.strftime("%G-W%V")

    syms = sorted(df.symbol.unique())
    ini = int(df.ts_ms.min() - 95 * D.DAY_MS)
    fin = int(df.ts_ms.max() + (H + 24) * D.HOUR_MS)
    print(f"  klines de {len(syms)} simbolos...", flush=True)
    K = P.bulk_vol(syms, "1h", ini, fin)
    btc = P.bulk_vol(["BTCUSDT"], "1h", ini, fin).get("BTCUSDT")
    de = max(int(d.open_time.iloc[-1]) for d in K.values())
    df = df[df.ts_ms <= de - H * D.HOUR_MS]

    # ── contexto del propio feed: solo alertas ANTERIORES ──
    prev_sym, ult_sym, n24 = [], [], []
    hist = {}
    todas = []
    for _, a in df.iterrows():
        t = int(a.ts_ms)
        h = hist.setdefault(a.symbol, [])
        prev_sym.append(sum(1 for x in h if t - x <= 7 * D.DAY_MS))
        ult_sym.append((t - h[-1]) / D.HOUR_MS if h else 999.0)
        while todas and t - todas[0] > D.DAY_MS:
            todas.pop(0)
        n24.append(len(todas))
        h.append(t)
        todas.append(t)
    df["f_alertas_prev_7d"] = prev_sym
    df["f_horas_desde_ult"] = np.minimum(ult_sym, 999.0)
    df["f_primera"] = (np.array(prev_sym) == 0).astype(float)
    df["f_feed_24h"] = n24

    rng = random.Random(seed)
    filas = []
    for _, a in df.iterrows():
        d = K.get(a.symbol)
        if d is None:
            continue
        i = F.engine_bar_index(d, int(a.ts_ms))
        if i is None:
            continue
        r = D.fwd(d, i, H)
        if r is None:
            continue
        at = P.atributos(d, i, btc, int(a.ts_ms))
        if at is None or any(v is None for v in at.values()):
            continue
        got, tries = [], 0
        while len(got) < darts and tries < darts * 8:
            tries += 1
            t = rng.randrange(int(a.ts_ms) - 21 * D.DAY_MS, int(a.ts_ms) + 21 * D.DAY_MS)
            if abs(t - int(a.ts_ms)) < 7 * D.DAY_MS:
                continue
            rr = D.fwd(d, F.bar_index(d, t), H)
            if rr is not None:
                got.append(rr)
        if len(got) < 4:
            continue

        cl, hi, lo_, qv = (d["close"].values, d["high"].values,
                           d["low"].values, d["qv"].values)
        px = float(cl[i])
        rng60_hi, rng60_lo = float(hi[i - 60:i].max()), float(lo_[i - 60:i].min())
        v24 = qv[i - 24:i].mean()
        v168 = qv[i - 168:i].mean()
        bj = F.bar_index(btc, int(a.ts_ms)) if btc is not None else None
        bcl = btc["close"].values if btc is not None else None

        row = {
            "symbol": a.symbol, "week": a.week, "ts": a.ts, "ts_ms": int(a.ts_ms),
            "signal": a.signal_type, "r": r, "dardo": float(np.mean(got)),
            # de la tabla
            "f_score": float(_num(pd.Series([a.score])).iloc[0]),
            "f_obv": float(_num(pd.Series([a.get("obv_slope")])).iloc[0]),
            "f_cvd": float(_num(pd.Series([a.get("cvd_ratio")])).iloc[0]),
            "f_recent_ok": float(a.get("recent_long_ok") in (True, "true", "True", 1)),
            "f_forming": float(str(a.get("candle_status")) != "closed"),
            "f_funding": float(_num(pd.Series([a.get("funding_rate")])).iloc[0]),
            "f_bucket": {"WATCH": 0.0, "STRONG": 1.0, "BEST": 2.0}.get(a.bucket, 0.0),
            # extension: cuanto por encima del nivel de referencia disparo
            "f_extension": (float(a.entry_price) / float(a.ref_price) - 1
                            if pd.notna(a.get("ref_price")) and float(a.ref_price) > 0
                            else np.nan),
            # del feed
            "f_alertas_prev_7d": float(a.f_alertas_prev_7d),
            "f_horas_desde_ult": float(a.f_horas_desde_ult),
            "f_primera": float(a.f_primera),
            "f_feed_24h": float(a.f_feed_24h),
            # de las klines
            "f_ret_24h": px / float(cl[i - 24]) - 1,
            "f_ret_72h": px / float(cl[i - 72]) - 1,
            "f_ret_168h": px / float(cl[i - 168]) - 1,
            "f_pos_rango60": ((px - rng60_lo) / (rng60_hi - rng60_lo)
                              if rng60_hi > rng60_lo else 0.5),
            "f_dist_max60": (rng60_hi - px) / px,
            "f_vol_ratio": float(v24 / v168) if v168 > 0 else 1.0,
            "f_hora_sin": float(np.sin(2 * np.pi * a.ts.hour / 24)),
            "f_hora_cos": float(np.cos(2 * np.pi * a.ts.hour / 24)),
            "f_dow": float(a.ts.dayofweek),
        }
        for k, v in at.items():
            row[f"f_{k}"] = float(v)
        if bj is not None and bj > 168:
            row["f_btc_24h"] = float(bcl[bj]) / float(bcl[bj - 24]) - 1
            row["f_btc_168h"] = float(bcl[bj]) / float(bcl[bj - 168]) - 1
            row["f_btc_vol"] = float(np.diff(np.log(bcl[bj - 168:bj + 1])).std())
        else:
            row["f_btc_24h"] = row["f_btc_168h"] = row["f_btc_vol"] = np.nan
        filas.append(row)

    T = pd.DataFrame(filas)
    T["margen"] = T.r - T.dardo
    # one-hot de la senal
    for s in sorted(T.signal.unique()):
        T[f"f_sig_{s}"] = (T.signal == s).astype(float)
    T.to_pickle(p)
    return T


def sumar_posicionamiento(T, lookback_d=14):
    """tt_pos y companía. Van de CONTROL NEGATIVO: ya se tumbaron."""
    try:
        import posicionamiento as PS
    except Exception as e:
        print(f"  (sin posicionamiento: {e})")
        return T
    ini = (T.ts.min() - pd.Timedelta(days=lookback_d + 2)).strftime("%Y-%m-%d")
    fin = (T.ts.max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    fechas = [d.strftime("%Y-%m-%d")
              for d in pd.date_range(ini, fin, freq="D", inclusive="left")]
    M, syms = {}, sorted(T.symbol.unique())
    for n, s in enumerate(syms, 1):
        try:
            f = PS.frame_simbolo(s, fechas)
        except Exception:
            f = None
        if f is not None and len(f) > 24 * lookback_d:
            M[s] = f
        if n % 100 == 0:
            print(f"    posic {n}/{len(syms)} ({len(M)} con datos)", flush=True)
    LB = 24 * lookback_d
    cols = {}
    for v in PS.VARS:
        cols[f"f_{v}"] = np.full(len(T), np.nan)
        cols[f"f_z_{v}"] = np.full(len(T), np.nan)
    for n, (_, a) in enumerate(T.iterrows()):
        m = M.get(a.symbol)
        if m is None:
            continue
        t = m["t"].values
        j = int(np.searchsorted(t, a.ts_ms - F.HOUR_MS, side="right")) - 1
        if j < LB:
            continue
        for v in PS.VARS:
            x = m[v].values[j - LB:j + 1].astype(float)
            if np.isnan(x).any() or x.std() == 0:
                continue
            cols[f"f_{v}"][n] = x[-1]
            cols[f"f_z_{v}"][n] = (x[-1] - x.mean()) / x.std()
    for k, v in cols.items():
        T[k] = v
    return T


# ════════════════════════════════════════════════════════════════════════════
# Evaluacion
# ════════════════════════════════════════════════════════════════════════════
def matriz(T, cols):
    X = T[cols].astype(float).values.copy()
    med = np.nanmedian(X, axis=0)
    med = np.where(np.isnan(med), 0.0, med)
    idx = np.where(np.isnan(X))
    X[idx] = np.take(med, idx[1])
    return X


def spearman(a, b):
    ra = pd.Series(a).rank().values
    rb = pd.Series(b).rank().values
    if ra.std() == 0 or rb.std() == 0:
        return 0.0
    return float(np.corrcoef(ra, rb)[0, 1])


def corrida(Xtr, ytr, Xva, yva, Xte, semilla=0, refit=True):
    """Devuelve {modelo: pred_test}. Hiperparametros por validacion INTERNA.

    `refit=False` usa el modelo con early stopping tal cual, sin reentrenar sobre
    train+val. Es lo que se usa en el null: cuesta la mitad y el null tiene que
    reflejar el mismo pipeline, no uno mejor.
    """
    out = {}
    mejor = (np.inf, 1.0)
    for al in (1.0, 10.0, 100.0, 1000.0, 10000.0):
        e = float(np.mean((yva - ridge(Xtr, ytr, Xva, al)) ** 2))
        if e < mejor[0]:
            mejor = (e, al)
    if refit:
        XT, yT = np.vstack([Xtr, Xva]), np.concatenate([ytr, yva])
    else:
        XT, yT = Xtr, ytr
    out["ridge"] = ridge(XT, yT, Xte, mejor[1])
    b = Boost(seed=semilla).fit(Xtr, ytr, Xva, yva)
    if refit:
        b = Boost(n=max(len(b.arboles), 1), seed=semilla).fit(XT, yT)
    out["boost"] = b.predict(Xte)
    return out


def walk_forward(T, X, y, frac, semilla=0, min_sem=5, refit=True):
    """Walk-forward expandido: para cada semana k se entrena con TODO lo anterior.

    Con 13 semanas, un solo corte 60/40 deja 3 semanas de test — y un IC por semana
    sobre 3 semanas no dice nada. Asi se consiguen ~7 semanas retenidas en vez de 3,
    y sigue siendo estrictamente causal: la semana k nunca ve nada de la semana k.
    """
    sem = sorted(T.week.unique())
    pred = {"ridge": np.full(len(T), np.nan), "boost": np.full(len(T), np.nan)}
    usadas = []
    W = T.week.values
    for k in range(min_sem + 1, len(sem)):
        itr = np.where(np.isin(W, sem[:k - 1]))[0]
        iva = np.where(W == sem[k - 1])[0]
        ite = np.where(W == sem[k])[0]
        if len(itr) < 150 or len(iva) < 20 or len(ite) < 20:
            continue
        pp = corrida(X[itr], y[itr], X[iva], y[iva], X[ite],
                     semilla=semilla, refit=refit)
        for nom, p in pp.items():
            pred[nom][ite] = p
        usadas.append(sem[k])
    return pred, usadas


def seleccion(T, pred, frac):
    """Top `frac` de CADA semana retenida. Es como se operaria: elegis lo mejor
    de lo que el feed te da esa semana, no un umbral global calibrado a posteriori."""
    idx = []
    for w, g in T.groupby("week"):
        p = pred[g.index.values]
        ok = ~np.isnan(p)
        if ok.sum() < 10:
            continue
        gi = g.index.values[ok]
        pp = p[ok]
        k = max(int(round(len(pp) * frac)), 1)
        idx.extend(gi[np.argsort(-pp)[:k]])
    return np.array(sorted(idx), dtype=int)


def informe(nom, T, idx, res):
    g = T.loc[idx]
    lo, hi = F.boot_ci(g.margen.tolist(), g.week.tolist())
    nw = g.week.nunique()
    k_s = max(3, int(0.05 * g.symbol.nunique()))
    k_w = max(1, min(3, nw - 3))
    d_sim, _, _ = F.drop_top(g.margen.tolist(), g.week.tolist(), g.symbol.tolist(), k=k_s)
    d_sem, _, _ = F.drop_top(g.margen.tolist(), g.week.tolist(), g.week.tolist(), k=k_w)
    m_sim = float(np.mean(d_sim)) if len(d_sim) else float("nan")
    m_sem = float(np.mean(d_sem)) if len(d_sem) else float("nan")
    print(f"  {nom:<24} n={len(g):>5} ({nw} sem)  margen {g.margen.mean()*100:>+7.2f}pp  "
          f"IC95 [{lo*100:>+7.2f},{hi*100:>+7.2f}]  "
          f"sin{k_s}sim {m_sim*100:>+6.2f}  sin{k_w}sem {m_sem*100:>+6.2f}  "
          f"ret {g.r.mean()*100:>+6.2f}%  {'CRUZA' if lo > 0 else '-'}")
    res[nom] = {"n": int(len(g)), "semanas": int(nw), "margen": float(g.margen.mean()),
                "ic": [float(lo), float(hi)], "cruza": bool(lo > 0),
                "sin_sim": m_sim, "sin_sem": m_sem}
    return lo > 0


def diagnostico(T, pred, frac, res):
    """Por que un decil puede dar +17pp y no significar nada.

    Dos tests pueden discrepar: el null de permutacion mira si ESTE pipeline supera
    a su propio ruido, y el bootstrap por semana mira si la media se despega del
    cero. Cuando discrepan, la respuesta esta casi siempre en la concentracion —
    que el null no esta disenado para ver.
    """
    print("\n" + "-" * 132)
    print("DIAGNOSTICO — de donde sale el margen del decil")
    print("-" * 132)
    ret = T.index.values[~np.isnan(pred["boost"])]
    d = {}
    for nom in ("ridge", "boost"):
        idx = seleccion(T, pred[nom], frac)
        g = T.loc[idx]
        s = g.margen.sort_values(ascending=False)
        sin3 = float(s.iloc[3:].mean())
        print(f"\n  {nom}: n={len(g)}")
        print(f"    media {g.margen.mean()*100:>+7.2f}pp   MEDIANA "
              f"{g.margen.median()*100:>+6.2f}pp   positivos {(g.margen>0).mean()*100:.0f}%")
        print(f"    las 3 alertas mas grandes aportan "
              f"{s.head(3).sum()/g.margen.sum()*100:>3.0f}% de la suma; sin ellas la "
              f"media queda {sin3*100:+.2f}pp")
        print("    y son: " + ", ".join(f"{T.loc[i,'symbol']} {v*100:+.0f}pp"
                                        for i, v in s.head(3).items()))
        print("    por semana: " + "  ".join(
            f"{w[-3:]}:{gg.margen.mean()*100:+.1f}" for w, gg in g.groupby("week")))
        d[nom] = {"media": float(g.margen.mean()), "mediana": float(g.margen.median()),
                  "positivos": float((g.margen > 0).mean()), "sin_top3": sin3,
                  "top3": [[T.loc[i, "symbol"], float(v)] for i, v in s.head(3).items()]}

    print("\n  PODER DE ORDENAMIENTO — quintiles de la prediccion en el retenido")
    print("    (si el modelo ordenara de verdad, la media SUBIRIA de q1 a q5)")
    for nom in ("ridge", "boost"):
        p, m = pred[nom][ret], T.margen.values[ret]
        q = pd.qcut(pd.Series(p).rank(method="first"), 5, labels=False).values
        med = [float(m[q == k].mean()) for k in range(5)]
        mdn = [float(np.median(m[q == k])) for k in range(5)]
        print(f"    {nom:<6} media  : " + "  ".join(f"q{k+1} {v*100:>+6.2f}"
                                                    for k, v in enumerate(med)))
        print(f"    {'':<6} mediana: " + "  ".join(f"q{k+1} {v*100:>+6.2f}"
                                                   for k, v in enumerate(mdn)))
        d[nom]["quintiles_media"] = med
        d[nom]["quintiles_mediana"] = mdn
    print("\n    Si la mediana BAJA de q1 a q5 mientras la media sube, el modelo esta")
    print("    ordenando por VOLATILIDAD, no por retorno esperado — que es justo la")
    print("    firma que dejaron las ~450 hipotesis anteriores.")
    res["diagnostico"] = d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizonte-h", type=int, default=168)
    ap.add_argument("--darts", type=int, default=10)
    ap.add_argument("--seed", type=int, default=17)
    ap.add_argument("--nulls", type=int, default=120)
    ap.add_argument("--frac", type=float, default=0.10, help="decil superior")
    ap.add_argument("--sin-posic", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    H = args.horizonte_h
    out = args.out or f"techo_h{H}.json"
    res = {"horizonte_h": H, "frac": args.frac, "nulls": args.nulls}

    print("=" * 132)
    print(f"TECHO CONDICIONAL — horizonte {H}h · objetivo = margen contra dardo pareado")
    print("=" * 132)
    T = cargar(CSV_SWING, H, args.darts, args.seed, tag="swing")
    if not args.sin_posic:
        print("  posicionamiento (control negativo)...", flush=True)
        T = sumar_posicionamiento(T)
    T = T.reset_index(drop=True)
    cols = sorted(c for c in T.columns if c.startswith("f_"))
    cols = [c for c in cols if T[c].notna().sum() > 50 and T[c].nunique(dropna=True) > 1]
    print(f"\n{len(T):,} alertas · {T.symbol.nunique()} simbolos · "
          f"{T.week.nunique()} semanas · {len(cols)} features")
    print(f"  margen global {T.margen.mean()*100:+.2f}pp  "
          f"(ret {T.r.mean()*100:+.2f}%  dardo {T.dardo.mean()*100:+.2f}%)")
    print("  por senal: " + "  ".join(
        f"{s}:{len(g)}/{g.margen.mean()*100:+.1f}pp" for s, g in T.groupby("signal")))

    X = matriz(T, cols)
    y = T.margen.values
    print("\n  walk-forward expandido: cada semana se predice con TODO lo anterior...",
          flush=True)
    t0 = time.time()
    pred, usadas = walk_forward(T, X, y, args.frac, semilla=args.seed)
    print(f"  {len(usadas)} semanas retenidas ({usadas[0]} -> {usadas[-1]}) "
          f"en {time.time()-t0:.0f}s")
    res["semanas_retenidas"] = usadas
    res["n"] = {"total": int(len(T)), "features": len(cols)}

    ret = T.index.values[~np.isnan(pred["ridge"])]
    print("\n" + "-" * 132)
    print("REFERENCIAS (sin modelo), sobre las mismas semanas retenidas")
    print("-" * 132)
    r0 = {}
    informe("todo el retenido", T, ret, r0)
    ps = np.full(len(T), np.nan)
    ps[ret] = np.nan_to_num(T.f_score.values[ret], nan=-1e9)
    informe("decil de score", T, seleccion(T, ps, args.frac), r0)
    ps2 = np.full(len(T), np.nan)
    ps2[ret] = np.random.default_rng(args.seed).normal(size=len(ret))
    informe("decil al azar", T, seleccion(T, ps2, args.frac), r0)
    res["referencias"] = r0

    print("\n" + "-" * 132)
    print(f"MODELOS — top {int(args.frac*100)}% de CADA semana retenida")
    print("-" * 132)
    obs, r1 = {}, {}
    for nom in ("ridge", "boost"):
        idx = seleccion(T, pred[nom], args.frac)
        obs[nom] = float(T.loc[idx].margen.mean())
        informe(nom, T, idx, r1)
        rho = spearman(pred[nom][ret], y[ret])
        print(f"  {'':<24} spearman(pred, margen) en retenido: {rho:+.4f}")
        r1[nom]["spearman"] = rho
    res["modelos"] = r1

    print("\n" + "-" * 132)
    print(f"NULL DE PERMUTACION — {args.nulls} corridas del pipeline COMPLETO "
          f"(walk-forward incluido) con el objetivo desplazado")
    print("-" * 132)
    print("  el desplazamiento circular preserva la autocorrelacion del retorno y rompe")
    print("  el vinculo con las features. El null corre sin el refit final (mitad de")
    print("  costo); eso lo hace algo CONSERVADOR, o sea que favorece al observado.")
    print("  el vinculo con las features: dice cuanto produce ESTE pipeline con ruido")
    pn = Path(f"techo_nulls_h{H}.json")
    nulos = json.loads(pn.read_text()) if pn.exists() else {"ridge": [], "boost": []}
    hechos = len(nulos["ridge"])
    if hechos:
        print(f"  {hechos} nulls ya acumulados en {pn.name}; se suman los nuevos")
    rng = np.random.default_rng(args.seed + 1000 * hechos)
    n = len(y)
    t0 = time.time()
    for b in range(hechos, hechos + args.nulls):
        yp = np.roll(y, int(rng.integers(int(0.05 * n), int(0.95 * n))))
        Tp = T.copy()
        Tp["margen"] = yp
        pp, _ = walk_forward(Tp, X, yp, args.frac, semilla=b, refit=False)
        for nom in nulos:
            idx = seleccion(Tp, pp[nom], args.frac)
            nulos[nom].append(float(Tp.loc[idx].margen.mean()))
        pn.write_text(json.dumps(nulos))
        k = b - hechos + 1
        if k % 5 == 0:
            el = time.time() - t0
            print(f"    {k}/{args.nulls} nuevos ({len(nulos['ridge'])} en total) · "
                  f"{el/k:.1f}s cada uno · faltan {(args.nulls-k)*el/k/60:.0f} min",
                  flush=True)
    res["null"] = {}
    for nom in nulos:
        v = np.array(nulos[nom])
        p95 = float(np.percentile(v, 95))
        pval = float((v >= obs[nom]).mean())
        print(f"  {nom:<10} observado {obs[nom]*100:>+7.2f}pp   null: "
              f"mediana {np.median(v)*100:>+6.2f}  p95 {p95*100:>+6.2f}  "
              f"max {v.max()*100:>+6.2f}   p={pval:.3f}  "
              f"{'SUPERA' if obs[nom] > p95 else 'no supera'}")
        res["null"][nom] = {"observado": float(obs[nom]), "p95": p95,
                            "mediana": float(np.median(v)), "p": pval,
                            "supera": bool(obs[nom] > p95)}

    print("\n" + "=" * 132)
    print("REGLA DE PARADA (preescrita): margen del decil positivo con IC95 sin cero "
          "Y por encima del p95 del null")
    print("=" * 132)
    cruzan = [k for k in nulos
              if res["modelos"][k]["cruza"] and res["null"][k]["supera"]]
    for k in nulos:
        print(f"  {k:<10} IC95 sin cero: {'SI' if res['modelos'][k]['cruza'] else 'NO':<3}"
              f"   supera el null: {'SI' if res['null'][k]['supera'] else 'NO'}")
    ver = ("HAY INFORMACION CONDICIONAL" if cruzan
           else "NO HAY INFORMACION CONDICIONAL APROVECHABLE EN ESTE FEED")
    res["veredicto"], res["cruzan"] = ver, cruzan
    print(f"\n  >>> {ver} <<<")

    diagnostico(T, pred, args.frac, res)

    print("\n" + "-" * 132)
    print("A QUE LE PRESTO ATENCION EL LINEAL (coef estandarizado, top 12) — descriptivo")
    print("-" * 132)
    mu, sd = X.mean(0), X.std(0)
    sd = np.where(sd > 1e-12, sd, 1.0)
    A = (X - mu) / sd
    w = np.linalg.solve(A.T @ A + 100.0 * np.eye(A.shape[1]), A.T @ (y - y.mean()))
    for j in np.argsort(-np.abs(w))[:12]:
        print(f"  {cols[j]:<26} {w[j]*100:>+8.3f}pp por desvio")
    res["coef"] = {cols[j]: float(w[j]) for j in np.argsort(-np.abs(w))[:12]}

    print("\n" + "-" * 132)
    print("BACKTRACK DESCRIPTIVO — quienes fueron los ganadores")
    print("-" * 132)
    q = T.nlargest(max(int(len(T) * 0.10), 20), "margen")
    top = q.symbol.value_counts().head(8)
    print(f"  decil ganador: {len(q)} alertas · {q.symbol.nunique()} simbolos distintos "
          f"(de {T.symbol.nunique()})")
    print(f"  top-8 simbolos = {top.sum()/len(q)*100:.0f}% del decil:  " +
          "  ".join(f"{s}:{c}" for s, c in top.items()))
    # el margen total es ~0, asi que una "participacion" no significa nada:
    # se muestra cuanto se mueve la MEDIA global al sacar los que mas aportan
    ap = T.groupby("symbol").margen.sum().sort_values(ascending=False)
    for k in (1, 3, 10):
        sub = T[~T.symbol.isin(ap.head(k).index)]
        print(f"  margen medio global sacando el top-{k:<2} aportante: "
              f"{sub.margen.mean()*100:>+6.2f}pp   (global {T.margen.mean()*100:+.2f}pp)")
    fuera = T[T.symbol.isin(top.index) & ~T.index.isin(q.index)]
    print(f"  los 8 simbolos mas frecuentes del decil ganador, en sus OTRAS "
          f"{len(fuera)} alertas: {fuera.margen.mean()*100:+.2f}pp")
    res["backtrack"] = {"simbolos_decil": int(q.symbol.nunique()),
                        "top8_pct": float(top.sum() / len(q)),
                        "repiten_fuera": float(fuera.margen.mean())}

    Path(out).write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nGuardado: {out}")


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    main()
