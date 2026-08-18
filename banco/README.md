# banco/ — banco de pruebas de señales

Contesta en minutos la única pregunta que importa de cualquier idea de trading
direccional: **¿suma win rate por encima del umbral, o no?**

No es el day trader (raíz) ni el swing (`swing/`) ni el basis (`basis/`, cerrado).
Es una herramienta de medición, agnóstica al proyecto.

```
py -3.13 primer_toque.py                       # línea base
py -3.13 primer_toque.py --target 10 --stop 5
py -3.13 primer_toque.py --regimen             # + corte por mes y detección
```

## Qué mide

Triple barrera / primer toque. Se entra en una vela y se camina hacia adelante hasta
tocar el target (+t%) o el stop (−s%). Lo que toque primero define el trade.

## Las dos fórmulas que evitan engañarse

**1. El win rate no es una habilidad, es una perilla.**

```
win rate necesario = (stop + costo) / (target + stop)
```

Con `+8% / −16%` sacás **64% de aciertos** sin ninguna señal, y perdés plata igual
(necesitás 67,5%). Con `+8% / −4%` sacás 32% y también perdés. Comparar contra el
umbral, **nunca contra 50%**.

**2. Una moneda al aire no da 50%, da ~52%.**

Con barreras simétricas en porcentaje, para recuperarte de −8% necesitás +8,7%: la
barrera de abajo queda más lejos en escala log. Para ±8% un activo **sin dirección**
da 52,00% y el umbral es 51,25% — o sea que un mercado plano regala +0,75pp.
`winrate_sin_direccion()` lo calcula solo.

## Línea base medida (ago-2025 → ago-2026, 186 pares, ±8%, límite 30d)

| | |
|---|---|
| entradas | 116.572 (cada 12h) |
| se resuelven | 94,9% · mediana **2,3 días** |
| win rate largo | **48,63%** (necesario 51,25%) |
| win rate corto | 51,37% |
| mediana por par | 48,06% — igual al agregado, **no es concentración** |

**El contexto importa:** esa ventana fue un bear brutal. BTC −45,47%, ETH −49,36%,
mediana de las alts **−69,80%**, solo 12% de las monedas en positivo.

## El hallazgo que reorienta

El régimen decide mucho más que la selección de moneda:

```
2025-11   34,68%   ← peor mes
2026-07   83,30%   ← mejor mes
```

Casi **50 puntos** de swing según el mes, contra los ~3 que sumaría una señal buena.
Pero cambia rápido (rachas buenas de mediana 1 semana) y **el detector obvio no lo
anticipa**: ningún bucket de "el mercado venía subiendo/bajando" cruza el umbral, y el
signo de la separación se da vuelta según el lookback (+1,95pp a 14d, −4,24pp a 30d,
+1,88pp a 60d). Signo inestable = ruido.

## Cómo probar TU señal

```python
from klines import load_panel
from primer_toque import tabla, evaluar_senal

panel = load_panel("2025-08-01", "2026-08-01", n=200, pin="base200")
T = tabla(panel, target=8, stop=8, horizonte_d=30)

mascara = mi_senal(T, panel)          # booleano por fila de T
evaluar_senal(T, mascara, "mi señal")
```

Imprime el win rate con y sin señal, cuántos pp aporta sobre la línea base, y si cruza
el umbral. **Si no cruza, no sirve** — por interesante que suene la idea.

## `lote.py` — probar 30 ideas de una, con la corrección que eso exige

Probar una idea por sesión es lento **y deshonesto**. Deshonesto porque una hipótesis
mirada sola siempre parece confirmatoria: el ojo ya recorrió veinte tablas antes de
elegir cuál reportar. Este repo ya se comió esa — un spread de +6,6pp con p≈1% que era
*look-elsewhere* sobre una tabla de ~30 celdas.

```
py -3.13 lote.py                        # batería estándar (30 hipótesis)
py -3.13 lote.py --cruces               # + cruces de a pares
py -3.13 lote.py --pares 300 --q 0.05   # más universo, más exigente
```

Con `features()` una idea nueva es una línea:

```python
from lote import features, lote
F = features(panel, T)
lote(T, {"mi idea": F.roc_168 > 0.3, "otra": F.dd_168 < -0.4})
```

### Las seis compuertas, cableadas en el código

El veredicto por default es **cerrada**: la carga de la prueba la tiene la señal.

1. **Muestra** — menos de 200 entradas resueltas es `POCA MUESTRA`, que **no es lo mismo
   que refutada** (ver trampa 6 abajo).
2. **Umbral** — win rate > el necesario. Aportar pp no alcanza.
3. **Multiplicidad** — Benjamini-Hochberg sobre el lote entero, q=0,10.
4. **Selección vs timing** — contra la línea base *del mismo símbolo*, no la global. Si
   una señal solo elige los pares que iban a andar bien igual, acá se ve.
5. **Concentración** — sigue arriba del umbral sin el top-3, y sin el mejor par solo.
6. **Consistencia semanal** — ≥60% de las semanas arriba del umbral.

### La corrida grande: 450 hipótesis, 0 sobreviven

`--cruces` cruza de a pares todas las colas de todas las features: **450 hipótesis en una
corrida de ~20 minutos**. Resultado sobre ago-2025 → ago-2026:

| | |
|---|---|
| no cruzan el umbral | 335 |
| mueren en la corrección | 91 |
| poca muestra | 24 |
| **sobreviven** | **0** |

Y el número que justifica todo el aparato:

| | |
|---|---|
| hipótesis con p < 0,05 **suponiendo independencia** | **68** |
| hipótesis con p < 0,05 con el remuestreo por semanas | **0** |

Probadas de a una y por sesión, esas 68 habrían sido 68 "hallazgos" con p < 0,05, cada uno
consumiendo una sesión antes de morir. Corridas juntas y con la corrección puesta, quedan
cero. Además las 12 mejores son todas cruces de **`mkt_vol_168 bajo`** con otra cosa: no
son 12 hallazgos, es **uno solo disfrazado doce veces**.

### El p-valor que importa es el de semanas

El binomial supone entradas independientes y acá **eso es falso**: hay una entrada cada
12h con horizonte de 30d, o sea ~60 trades vivos a la vez, y el régimen está
autocorrelacionado. `_p_bloques()` remuestrea **semanas enteras, cada una pesando igual**.

> **Corrección 2026-08-17.** La primera versión remuestreaba bloques de semanas pero
> **pooleaba las entradas** de cada bloque, así que las semanas con más entradas pesaban
> más y, con pocas semanas, subestimaba la variabilidad. En `fade/evaluar.py` (8 semanas)
> eso dio vuelta un veredicto: IC [+0,17%, +2,32%] contra el correcto [−0,52%, +3,30%].
> Acá, con ~52 semanas, el sesgo era menor pero real: `rango_168 bajo` pasó de p=0,32 a
> **p=0,60**.

La diferencia no es cosmética. En la primera corrida:

| | p independiente | p de bloques |
|---|---|---|
| `mkt_vol_168 bajo` (+9,98pp) | 0,0000 | **0,1415** |

Esa hipótesis —volatilidad del mercado baja— tenía el margen más grande del lote,
sobrevivía concentración y ganaba +12,7pp contra su línea base pareada. Con el p-valor
ingenuo era un hallazgo redondo. Con el correcto **no se distingue de ruido**: es una
señal de *timing de mercado*, y las semanas de baja volatilidad vienen en tandas, así que
el n efectivo es una fracción de las 21.799 entradas contadas.

Esa brecha entre los dos p-valores es, literalmente, el autoengaño.

### Reproducibilidad

`universe()` consulta el ranking de volumen **en vivo**, así que dos corridas separadas
por horas usan universos distintos y la línea base se mueve sola (se vio: 48,63% → 48,71%
entre dos corridas del mismo lote). Por eso `load_panel(..., pin="base200")` congela la
lista en disco. Con el pin puesto, dos corridas dan un CSV byte a byte idéntico — y el
panel carga en **1 segundo** en vez de 113.

## Disciplina de medición (por qué está cableada en `evaluar()`)

Sin esto, en este repo ya se dieron cinco hallazgos falsos por buenos:

1. **Mediana además de media** — la media siempre está cargada por la cola derecha.
2. **Chequeo de concentración** — recalcular sin los 3 pares que más aportan.
   BANKUSDT dio vuelta cinco resultados del swing y volvió a aparecer en el basis.
3. **Partir por semana** — si el resultado vive en una sola, es ruido.
4. **Costos desde el día uno** — antes de ago-2026 ningún número del repo los incluía.
5. **Ventana fija con fechas explícitas** — `--weeks` es relativo a hoy; dos corridas
   separadas cubren períodos distintos y eso solo ya fabricó artefactos de varios pp.
6. **Regla de parada escrita antes de correr** — si un experimento no puede cambiar la
   decisión, no correrlo.

## Sesgos declarados

- **Universo = top por volumen de HOY.** Los pares deslistados no están; los que
  sobrevivieron son a los que mejor les fue. Sesga hacia mejor. (Y aun así cayeron 70%.)
- **Costos = solo fees** (0,20% ida y vuelta spot taker). Sin slippage, que en pares
  finos es varias veces eso.
- **Un año, un régimen.** Los cortes por mes/semestre son pocas observaciones.
- **Entradas solapadas** — no sesga la media, sí infla la aparente precisión.

## Nota de implementación

El cache mira `.parquet` **y** `.csv`: si el write de parquet falla se guarda csv, y
chequear solo parquet hace que el cache nunca pegue y se re-descargue todo en cada
corrida (eso convertía una corrida de segundos en una de 7 minutos).
