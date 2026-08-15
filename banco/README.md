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

panel = load_panel("2025-08-01", "2026-08-01", n=200)
T = tabla(panel, target=8, stop=8, horizonte_d=30)

mascara = mi_senal(T, panel)          # booleano por fila de T
evaluar_senal(T, mascara, "mi señal")
```

Imprime el win rate con y sin señal, cuántos pp aporta sobre la línea base, y si cruza
el umbral. **Si no cruza, no sirve** — por interesante que suene la idea.

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
