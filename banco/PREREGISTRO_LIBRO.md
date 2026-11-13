# PREREGISTRO — el libro de órdenes como ranking transversal

> Escrito el **2026-10-30**, **antes de bajar el dato**. Del libro histórico se vio una
> sola cosa: el formato de un archivo (`BTCUSDT-bookDepth-2026-10-28`, encabezado y dos
> timestamps) y que los archivos existen. **Ningún retorno, ninguna feature calculada,
> ninguna estadística.** Si algo de este archivo cambia después de ver un número, el
> experimento no vale.

---

## 1. Por qué esta fuente no comparte causa de muerte con lo cerrado

La dirección dio **0 de 4.140**, y lo que tienen en común todos los intentos es la
**fuente**: la vela (precio, volumen, flujo agresor del kline), el posicionamiento de
futuros (`metrics`), funding, on-chain, ETF, macro. Las familias de microestructura que se
cerraron tampoco tocaron esto:

| ya cerrado | qué miró | por qué no es el libro |
|---|---|---|
| `PREREGISTRO_MICRO` (0/192) | la forma del camino dentro de la vela | sale del OHLCV de 5m: son **trades ya hechos** |
| `PREREGISTRO_MAKER` | selección adversa del maker | `aggTrades`: también trades hechos |
| `libro.py` | spread y slippage | lo usó como **costo**, nunca como ranking |

**El libro es lo único del mercado que muestra intención no ejecutada**: cuánta plata
espera comprar y vender, y a qué distancia del precio. Todo lo demás del repo es
información que ya se ejecutó. `radar/HANDOFF_FORWARD_TEST.md` §7 lo dejó anotado como
"medido pero nunca rankeado".

### El dato existe, y es histórico

Binance Vision publica `bookDepth` de los perpetuos USDT-margen:
`data/futures/um/daily/bookDepth/{SYM}/{SYM}-bookDepth-{fecha}.zip`.

- **Cada 30 segundos**, la **profundidad acumulada** de cada lado a ±0,2 %, ±1 %, ±2 %,
  ±3 %, ±4 % y ±5 % del precio, en contratos (`depth`) y en USD (`notional`).
  Porcentaje negativo es el lado comprador (bid), positivo es el vendedor (ask).
- **Desde el 2023-01-01.** El 2022-12-25 da 404 y el 2023-01-01 da 200 (BTCUSDT). Pares medianos de los 46 (PENDLE, OP, TRX, ONDO, WIF, 1000BONK,
  1000PEPE) verificados con archivo en 2024.
- Solo diarios: los mensuales dan 404.

No hay que juntarlo hacia adelante como la prima: **se puede contestar con historia**, y
con la maquinaria de `ranking.py` tal como corrió la corrida 3.

---

## 2. Diseño — el de `PREREGISTRO_TRANSVERSAL` corrida 3, sin cambios de método

**Universo:** los 46 de `deriv46` (`radar/medir.py`, `UNIVERSO_PRE`), con el símbolo del
perpetuo cuando difiere (`1000BONKUSDT`, `1000PEPEUSDT`, `1000SHIBUSDT`). Los que listaron
perpetuo después de 2023-01 entran desde su primer archivo; las barras con menos de
`MIN_SYMS = 30` válidos se descartan, como siempre.

**Ventana principal:** 2023-01-01 → **2026-07-31** (~187 semanas).
**Reserva fuera de muestra:** 2026-08-01 → 2026-10-31 (13 semanas). **No se baja ni se
mira** salvo que algo sobreviva a la principal.

**Barra, horizonte, k:** `paso = horizonte = 4h`, `k = 8`.
- 4h porque es el horizonte donde la magnitud se midió más fuerte (`horizonte_util.py`), y
  porque la información del libro, si existe, decae rápido. **Un solo horizonte**: no se
  barren 1h ni 24h.
- k = 8 sobre 46, igual que la corrida 3 y que el radar.

**Agregación horaria, sin lookahead:** de cada hora se toma el **último snapshot** con
timestamp ≤ cierre de la vela, como hace `metricas.feat_metricas` con `metrics`. Lo que
se sabe cuando la vela cierra, nada más. Los snapshots crudos no se guardan: solo el
panel horario.

### Los brazos — 8 features, fijadas ahora

Con `B(x)` y `A(x)` = notional acumulado del bid y del ask a x % del precio:

| feature | definición | la idea |
|---|---|---|
| `imb_02` | `(B−A)/(B+A)` a 0,2 % | presión pegada al precio |
| `imb_1` | ídem a 1 % | presión cercana |
| `imb_5` | ídem a 5 % | la pared lejana |
| `imb_1_m24` | media de `imb_1` en las últimas 24 h | presión persistente, no de un snapshot |
| `imb_1_chg4` | `imb_1` menos su valor de hace 4 h | presión que cambia |
| `imb_1_pct` | percentil de `imb_1` contra su propia historia de 720 h | comparable entre monedas |
| `prof_1_rel` | `(B+A)` a 1 % contra su mediana de 168 h | el libro se vacía o se llena |
| `prof_1_pct` | percentil de `(B+A)` a 1 % contra su propia historia de 720 h | ídem, comparable |

Cada una entra **cruda y residualizada contra `roc_24`**, y **en las dos direcciones**
(`[bajo]`), como manda `ranking.scores()`. Más los **3 controles al azar**. Son
8 × 2 × 2 + 3 = **35 rankings × 3 objetivos = 105 brazos**, todos al mismo FDR.
*(93 después de la enmienda del 2026-10-31: ver abajo.)*

**Objetivos:** `largo`, `corto` y `magnitud`, los tres siempre.

> **Sobre comparabilidad**, la lección de la corrida 3 §3: el `notional` en crudo rankea
> "qué moneda es" (BTC siempre tiene más libro que ONDO). Por eso `prof_1_rel` y los
> `_pct` existen, y **un sobreviviente que sea solo de nivel crudo es sospechoso** de
> rankear identidad. Los `imb_*` son razones y ya son comparables.

---

## 3. Compuertas — las de siempre, sin tocar

Las siete de `PREREGISTRO_TRANSVERSAL` §3: semanas ≥ 8 · signo > 0 · bootstrap de
semanas p < 0,05 · **Benjamini-Hochberg q = 0,10 sobre los 105 brazos** · sin top-3 y sin
el mejor par · ≥ 60 % de semanas positivas · costo **0,20 % y 0,50 %** (un brazo que solo
vive a 0,20 % no cuenta). Más la de **artefacto de escala** de la corrida 1 y el
normalizador `atr_base` de la corrida 2.

**Primero la nula:** `ranking.py --nula` con el mismo panel y el mismo tablero, y de su
dispersión semanal sale el MDE. Recién con el MDE en la mano se mira un brazo. Como
referencia (no como compuerta): la corrida 3 tuvo ±0,062 ATR con 251 semanas; con ~187
debería quedar cerca de ±0,07.

---

## 4. Régimen — lo que decide si un sobreviviente vale algo

La ventana cubre la recuperación de 2023, el bull de 2024, 2025 y el bear de 2026.
**Un sobreviviente de `largo` o `corto` se parte por año calendario (2023, 2024, 2025,
2026) y no puede cambiar de signo en ninguno.** Si sobrevive ahí, recién entonces se baja
y se corre la reserva de §2.

---

## 5. FUGA DECLARADA — lo que ya sé antes de correr

1. **El prior, y es bajo para la dirección.** La literatura de microestructura encuentra
   que el desbalance del libro predice el precio a **segundos o minutos**, y que esa
   información se agota mucho antes de 4 horas. Si a 4h no queda nada, es lo esperable,
   no una sorpresa.
2. **El prior es alto para la magnitud, y por eso no cuenta como hallazgo.** Libro fino y
   volatilidad van juntos casi por definición. Si `prof_1_rel [bajo]` sobrevive en
   `magnitud`, **es confirmatorio**: dice lo que el radar ya sabe, desde otra fuente.
   Solo sería nuevo si le gana al `n_surge` del radar (ver §6.3).
3. Vi el encabezado y las dos primeras filas de un archivo de BTC, para conocer el
   formato. Ninguna estadística de nada.

---

## 6. Regla de parada — fijada ANTES de bajar el dato

1. **Si ningún brazo se separa de los controles al azar por más del MDE** → el libro no
   ordena transversalmente a 4h, y la fuente queda **cerrada para esta forma de uso**.
   **No se prueba otro horizonte ni otra profundidad para compensar.**
2. **Si sobrevive algo en `largo` o `corto`** → es el primer resultado direccional del
   repo y **no se cree**: §4 (no cambiar de signo por año) y después la reserva. Aun
   sobreviviendo las dos, lo que sigue es un forward test con fecha y sin capital.
3. **Si sobrevive solo en `magnitud`** → se corre una vez más, **residualizado contra
   `n_surge`**. Si no sobrevive a eso, es el radar visto desde el libro y no agrega
   nada. Si sobrevive, el libro trae información de magnitud que la actividad no tiene,
   y eso **sí** importa para los straddles.
4. **Si sobrevive un nivel crudo y no su `_pct` o su `_rel`** → sospechoso de rankear
   identidad de moneda: se exige `sin_top3` y `sin_top1` positivos, que ya están
   cableados.
5. **El universo de 46 sobrevivientes grandes se declara en cualquier conclusión.** No se
   extrapola a la cola.

---

## 7. Lo que hay que construir, y el costo

- `banco/libro_hist.py` — baja los diarios de `bookDepth`, agrega a horario en el momento
  (último snapshot ≤ cierre) y **tira el crudo**. Caché en `banco/.libro_cache/`, fuera de
  git, como `.kline_cache/`. Expone `feat_libro(L, TB[["sym","t"]])`, con la misma firma
  que `feat_metricas`.
- `ranking.py --libro` — el mismo enchufe que `--derivados`, y las 8 features agregadas a
  la lista de `scores()`.
- **El tamaño:** 46 pares × ~1.300 días ≈ **60.000 archivos**, del orden de 20-30 GB
  comprimidos para bajar, pero el panel horario que queda pesa unos MB. Se baja una sola
  vez, en una corrida larga y sola (regla de `CLAUDE.md`: las corridas pesadas, de a una).
  Si la notebook está a batería, `powercfg /change standby-timeout-ac 0` y enchufada.

**Antes del lote, la verificación de que el dato es lo que dice ser:** contar la
cobertura por par y por mes (`--solo-conteo`) y chequear que el último snapshot de cada
hora caiga dentro de esa hora. Si un par tiene huecos de más del 10 %, se declara; no se
rellena.

---

## ENMIENDA del 2026-10-31 — `imb_02` sale, antes de calcular nada

Escribiendo el descargador apareció un dato del **formato**, no de ningún resultado: **el
nivel de ±0,2 % no existe en los archivos hasta 2026**. Hasta el 2026-01-01 los archivos
traen solo 1 a 5 % (y con el porcentaje como entero, `-5` y no `-5.00`); el 2026-03-01 ya
trae 0,2 %. Son ~5 meses de los 3,6 años de la ventana: no alcanza ni para el corte por
año de §4.

**Se saca `imb_02` y no se reemplaza por nada.** Agregar otra feature sobre la marcha
sería exactamente el tipo de decisión que el preregistro existe para evitar. Quedan **7
features → 28 rankings + 3 controles = 31 × 3 objetivos = 93 brazos**, todos al mismo FDR.
El resto del diseño no cambia.

Al momento de esta enmienda no se calculó ninguna feature sobre la ventana, ni ningún
retorno. Lo único que se corrió fue la prueba del descargador (BTC y PEPE, enero de 2023 y
marzo de 2026): cobertura y rangos, nada más.

---

# ────────────────── RESULTADOS (debajo de esta línea) ──────────────────

## RESULTADOS del 2026-11-12 — dirección muerta, magnitud viva y no es solo el radar

### El dato (§7)

`libro_hist.py --solo-conteo`: **1.978 par-meses, 46 de 46 pares**, 2023-01 → 2026-07. La
reserva no se bajó. Cobertura de horas con snapshot completo **99,0 % a 99,9 %** por par,
**0 días fallados**, ningún par con huecos de más del 10 %. El último snapshot de cada hora
cae a 29-30 s del cierre (p50/p95), adentro de la hora como pedía §7. Los pares que arrancan
en 2023-01 tienen un mes bajo el 90 %; los que listaron después, ninguno.

### Cómo se corrió

```
py ranking.py --libro --nula --pin deriv46 --pares 46 --k 8 --paso 4 --horizonte 4 \
              --inicio 2023-01-01 --fin 2026-08-01
py ranking.py --libro        (mismos argumentos, sin --nula)
py ranking.py --libro --neutralizar n_surge   (mismos argumentos; §6.3)
```

Tablero: **332.412 filas · 7.668 barras de 4h sin solape · 46 pares · 183 semanas**.
Cobertura de features del libro: **98,2 %** de las filas. 93 brazos, como fijó la enmienda.

Se corrió con Python 3.14 + pandas 2.3.3, no con el `py -3.13` del encabezado de los
scripts (la sesión no tenía 3.13). La corrida §6.3 se hizo con un script equivalente a
`main()` que llamaba a `scores(neutralizar="n_surge")`; el flag `--neutralizar` se agregó
después, en el mismo commit que este resultado, para que se pueda repetir.

### La nula (§3), antes de mirar ningún brazo

| objetivo | sd semanal | spread del azar | MDE |
|---|---|---|---|
| largo | 0,0668 | −0,1648 | **±0,0138 ATR** |
| corto | 0,0681 | −0,1636 | **±0,0141 ATR** |
| magnitud | 0,0651 | −0,0014 | **±0,0135 ATR** |

La referencia de §3 (~±0,07) estaba mal extrapolada: venía de la corrida 3, con barras de
24h. Con barras de 4h hay seis por día en vez de una y la sd semanal cae de ~0,35 a ~0,07.
Era referencia, no compuerta; no se cambió nada.

### Dirección: 0 de 62 — §6.1 se cumple, la fuente queda cerrada para dirección

Ningún brazo de `largo` ni de `corto` queda con spread positivo después del costo; las
semanas positivas no pasan del 4 %. Los dos que más se separan del azar son de `largo`
(`prof_1_pct` +0,018 y `imb_5` +0,014 sobre el control, en el borde del MDE), y los dos
siguen negativos netos. **El libro no ordena dirección a 4h**, que es lo que anticipaba
§5.1. No se prueba otro horizonte ni otra profundidad. La corrida a costo 0,50 % no se hizo:
solo decide algo si sobrevive un brazo de dirección.

### Magnitud: 14 de 31 sobreviven (contra `roc_24`)

| ranking | spread | sin top-3 | sem > 0 |
|---|---|---|---|
| `prof_1_rel` | **+0,160** | +0,125 | 83 % |
| `prof_1_pct` | +0,160 | +0,120 | 78 % |
| `prof_1_rel ~ sin roc_24` | +0,113 | +0,084 | 80 % |
| `imb_1_pct [bajo]` | +0,095 | +0,080 | 83 % |
| `imb_1 [bajo]` | +0,073 | +0,060 | 74 % |
| `imb_1_chg4 [bajo]` | +0,067 | +0,057 | 84 % |

(más 8 brazos residualizados entre +0,02 y +0,05). `imb_5 [bajo]` (+0,087) cae por
**artefacto de escala** (atr_ratio 0,92).

**El signo salió al revés del prior de §5.2.** Lo esperado era `prof_1_rel [bajo]` —libro
fino, más movimiento—, y ese brazo muere en el FDR (+0,016, p 0,084). Lo que sobrevive es el
libro **más profundo que su propia mediana**. La lectura obvia es actividad: cuando entra
flujo, los market makers también cargan el libro. Por eso §6.3 era la prueba que decidía.

### §6.3: residualizado contra `n_surge` — 7 de 31 sobreviven

| ranking | crudo | **~ sin n_surge** | sin top-3 | sem > 0 |
|---|---|---|---|---|
| `prof_1_rel` | +0,160 | **+0,093** | +0,067 | 74 % |
| `prof_1_pct` | +0,160 | **+0,076** | +0,057 | 64 % |
| `imb_1_chg4 [bajo]` | +0,067 | +0,034 | +0,030 | 69 % |
| `imb_1 [bajo]` | +0,073 | +0,030 | +0,023 | 60 % |
| `imb_1_pct [bajo]` | +0,095 | +0,028 | +0,021 | 68 % |
| `prof_1_rel [bajo]` | +0,016 | +0,027 | +0,020 | 63 % |
| `imb_1_chg4` | +0,043 | +0,015 | +0,010 | 63 % |

Por la letra de §6.3: **el libro trae información de magnitud que la actividad no tiene.**
`prof_1_rel` conserva ~58 % de su spread (+0,093 de +0,160) después de sacarle `n_surge`; el
desbalance (`imb_*`) aporta poco, apenas sobre el MDE. **No le gana al radar** (§5.2): +0,16
crudo contra el +0,51 de `n_surge` (no es la misma corrida y la comparación es aproximada).
Es información que se suma a la del radar, no una que la reemplaza. Se declara §6.5: vale
para los 46 sobrevivientes grandes y no se extrapola a la cola.

**Límite declarado.** La residualización es lineal por barra sobre z-scores. `n_surge` tiene
cola pesada, y parte de su efecto podría quedar en el residuo. Probarlo con rango o log
sería un test que este preregistro no fija, y no se corrió.

### Lo que sigue (no lo decide este documento)

§6.3 termina en "importa para los straddles" y no fija un paso posterior. Lo que corresponde
es un **preregistro nuevo** para el apareo con el radar (p. ej. el top-8 por `n_surge` +
`prof_1_rel`), con la combinación, la mejora mínima y el uso de la reserva 2026-08 → 10
fijados **antes** de bajarla. La reserva sigue sin mirar.
