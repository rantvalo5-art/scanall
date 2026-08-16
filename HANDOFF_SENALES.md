# HANDOFF — Lo que queda por probar (y con qué regla se cierra)

> Abrir en una conversación nueva y empezar por la sección 5.
> Escrito el **2026-08-15**, después de medir y cerrar cuatro líneas de trabajo.
> Complementa `HANDOFF_BASIS.md` (basis/funding, cerrado).

---

## 0. Regla de parada GLOBAL — leer antes que nada

Este repo lleva meses de construir antes de medir. La disciplina que lo dio vuelta fue
**escribir el criterio antes de mirar los números**. Se mantiene:

> **Cada ítem de la sección 4 tiene su regla de parada ya escrita. No se renegocia
> después de ver el resultado.** Si un experimento no puede cambiar la decisión, no
> se corre.

Y un límite que no existía antes:

> **Presupuesto total: 4 sesiones.** Si al agotarlas nada cruzó su umbral, se cierra la
> búsqueda de ventaja direccional y el capital queda en el piso (prestar stablecoins,
> 5-10% anual). Reabrir requiere una hipótesis nueva, no otra variante de las de acá.

**No hay capital operando hoy.** Nada sangra mientras se decide. Esto es investigación,
no rescate.

---

## 1. Lo que ya está medido y muerto — NO volver a probar

| línea | veredicto | número que lo cerró |
|---|---|---|
| Swing (predecir dirección con velas) | sin ventaja | −1,61% vs BTC; universo −1,65%; azar −0,79% |
| Basis / funding de perpetuos | bajo el piso | BTC +3,35% bruto anual; mediana del universo −4,71% |
| Basis de futuros con vencimiento | bajo el piso | BTC dic-2026 +4,45% anual (converge seguro, pero chico) |
| Day trader (alertas reales, con costos) | **peor que al azar** | −0,86pp a 4h, −3,27pp a 24h; ranking INVERTIDO |
| Selección de moneda (primer toque ±8%) | suma ~0 | win rate 48,63% vs 51,25% necesario |
| Corte transversal / momentum relativo | no replica a 5 años | **0/35 combos** positivos por media winsorizada |
| Reversión de corto plazo | = al costo | edge bruto +0,31% máx contra 0,40% de costo |
| Perseguir memes que pumpean | negativo | −4,2pp vs azar; tamaño chico no cambia el signo |
| Entradas a dos puntas (straddle sintético) | imposible estructuralmente | se regala k·ATR por trade |

**Contexto que hay que tener presente:** la mediana de los pares USDT cayó **−2,81%
cada 14 días durante 5 años**. Ninguna estrategia larga-sola sobre alts sobrevive eso.
Todo lo que quede tiene que ser neutral o corto.

---

## 2. El marco de medición — usarlo, no reinventarlo

Vive en **`banco/`**. Correr desde ahí:

```
py -3.13 primer_toque.py [--target 8 --stop 8 --regimen]
```

Para probar una señal nueva:

```python
from klines import load_panel
from primer_toque import tabla, evaluar_senal
panel = load_panel("2025-08-01", "2026-08-01", n=200)
T = tabla(panel, target=8, stop=8, horizonte_d=30)
evaluar_senal(T, mi_mascara, "mi señal")
```

Imprime cuántos pp aporta y **si cruza el umbral**, que es lo único que decide.

### Las dos fórmulas que evitan engañarse

```
win rate necesario = (stop + costo) / (target + stop)
```

El win rate **es una perilla, no una habilidad**: con +8%/−16% sacás 64% de aciertos sin
ninguna señal y perdés plata igual. Comparar contra el umbral, nunca contra 50%.

Y: **un activo sin dirección da ~52%, no 50%** (para recuperarte de −8% necesitás +8,7%;
la barrera de abajo está más lejos en escala log).

---

## 3. Las trampas que ya mordieron — con nombre y apellido

1. **Concentración.** Todo promedio positivo se rechequea sacando el top-1/3/5. Dio
   vuelta resultados **cuatro veces**: BANKUSDT (swing, dos veces), 币安人生USDT +
   BANKUSDT (basis), ZECUSDT + DEXEUSDT + STOUSDT (transversal, de +27,5% a +0,1%).
2. **Media contra mediana — y cuál manda.** LUNAUSDT (may-2022) cayó a $0,0000001 y el
   retorno forward daba **+5.199.900%**, contaminando una rejilla entera. Usar mediana +
   media winsorizada a ±100%. **Pero para una cartera el P&L es la MEDIA**: si mediana y
   media se contradicen, gana la media. La mediana es chequeo de robustez, no resultado.
3. **Ventana relativa a hoy.** Siempre fechas explícitas. `--weeks` fabricó artefactos de
   varios puntos porcentuales.
4. **Costos desde el día uno.** Antes de ago-2026 ningún número del repo los incluía. El
   backtest de la raíz **todavía no los tiene**.
5. **Cache que no pega.** `banco/klines.py` mira `.parquet` **y** `.csv`. Chequear solo
   uno hacía re-descargar 186 monedas por corrida (7 min → 2 s).
6. **Subpotenciado ≠ refutado.** El transversal daba +27,5% con 1 año y 0/35 con 5. Si un
   resultado depende de 3 nombres de 149, no se puede distinguir de cero: pedir más datos
   antes de creerlo *o* descartarlo.
7. **Matar el vigilante junto con el vigilado.** Un loop `until grep` quedó huérfano horas
   después de matar el proceso que miraba.

---

## 4. Lo que queda — ordenado por lo que yo probaría primero

### 4.1 — Fadear el propio screener  ·  esfuerzo: 1 sesión  ·  prior: medio

**Hipótesis.** El ranking del day trader está invertido: BEST rinde **peor** que WATCH
(mediana a 4h: −1,786% contra −0,878%). Si esa inversión es real y persiste, es
información aprovechable — evitar o fadear lo que el bot marca como mejor.

**El problema a resolver primero.** Puede ser **composición**, no señal: EXPLOSION es la
peor señal (−1,996%) y probablemente se concentra en BEST. Hay que separar el efecto del
score del efecto del tipo de señal.

**Qué medir.** `daytrader_outcomes` en Supabase (10.157 filas, ya explorada). Regresión o
corte cruzado score × signal_type. Comparar contra la línea base de azar de la misma
ventana (−0,205% a 4h, −0,288% a 24h).

> **Regla de parada.** Sigue solo si, **controlando por tipo de señal**, el gradiente por
> score se mantiene monótono e invertido, con mediana consistente en ≥6 de 8 semanas y
> sobreviviendo sacar el top-3 de símbolos. Si la inversión desaparece al controlar por
> señal, era composición: se cierra.

**Ojo:** la ventana son 50 días. Aunque pase, hace falta forward-test antes de operar.

---

### 4.2 — Funding extremo como señal contraria  ·  esfuerzo: 1 sesión  ·  prior: medio-bajo

**Hipótesis.** La Fase 1 midió el funding como *ingreso a cobrar* y murió porque el nivel
es chico. Nunca se midió como **sentimiento**: funding muy positivo = longs apalancados
amontonados = posible reversión.

**Ventaja práctica.** Los datos **ya están cacheados** en `basis/.funding_cache/`
(351 símbolos, ago-2025 → ago-2026). No hay que bajar nada.

**Qué medir.** Máscara sobre el banco: entradas donde el funding trailing está en el
percentil 95+ (o 5−). Pasar por `evaluar_senal()`.

> **Regla de parada.** Sigue solo si cruza el umbral de rentabilidad (no basta con sumar
> pp), con mediana por semana positiva y sobreviviendo el top-3. Y **el sesgo de un año
> bear hay que declararlo**: el funding extremo positivo es raro en bear.

---

### 4.3 — Detectores de régimen alternativos  ·  esfuerzo: 1-2 sesiones  ·  prior: bajo

**Por qué sigue vivo.** El régimen **domina** el resultado — el win rate mensual va de
34,68% (nov-2025) a 83,30% (jul-2026), casi 50 puntos, contra los ~3 que sumaría una
señal de selección. Es la variable que decide.

**Por qué es difícil.** Se probó **una** familia (retorno pasado del mercado) y falló
limpio: ningún bucket cruza el umbral y **el signo se da vuelta según el lookback**
(+1,95pp a 14d, −4,24pp a 30d, +1,88pp a 60d). Signo inestable = ruido.

**Qué falta probar.** Régimen de volatilidad (ATR agregado), amplitud (% de monedas sobre
su media móvil), estructura de correlación (correlación media entre pares), funding
agregado como termómetro. **Y el problema del lag:** el detector solo puede usar datos ya
resueltos; si el régimen dura 2-3 semanas (rachas buenas de mediana **1 semana**) y el
detector tarda 2 en confirmar, llegás tarde por construcción.

> **Regla de parada.** Sigue solo si algún detector cruza el umbral **con el mismo signo
> en al menos 3 lookbacks distintos**. Un solo lookback ganador es pesca. Máximo
> 2 sesiones: si ninguna familia cruza, el régimen queda como "real pero no anticipable"
> y se cierra.

---

### 4.4 — Vender volatilidad con opciones  ·  esfuerzo: 2+ sesiones  ·  prior: incierto

**Por qué es distinto de todo lo anterior.** Es el único lugar donde la habilidad
*demostrada* del screener tiene comprador natural. Está medido que predice **cuánto** se
mueve una moneda (el score duplica las dos colas) y que no predice **para qué lado**. Las
opciones pagan exactamente por lo primero.

**Las contras, sin maquillar.** Liquidez real solo en BTC y ETH. Vender opciones sin
cubrir puede costar en un día lo de un año. Es otra infraestructura (Deribit o Binance
Options), otro modelo de riesgo, y no se prueba con `banco/`.

> **Regla de parada.** Antes de escribir una línea de código: comparar la volatilidad
> implícita contra la realizada en BTC/ETH sobre 2 años. Si la implícita no supera a la
> realizada por un margen que cubra comisiones **y** el riesgo de cola, se cierra ahí. Es
> el equivalente al cálculo de la sección 3 del handoff de basis: **hacer la cuenta antes
> de construir.**

---

### 4.5 — Funding entre exchanges  ·  esfuerzo: 2 sesiones  ·  prior: bajo

Cobrar la **diferencia** de funding entre Binance / Bybit / OKX en vez del nivel — que es
justamente lo que falló por ser muy chico. Delta-neutral.

**Contras:** capital partido en dos exchanges, transferencias, el doble de superficie
operativa, y la diferencia también está competida. Además `fapi.binance.com` está
geo-bloqueado desde runners cloud (451/403) → hace falta PC propia o VPS.

> **Regla de parada.** Medir primero la **diferencia histórica** entre exchanges neta de
> los costos de las cuatro patas. Si no supera el piso de stablecoins con mediana positiva
> por símbolo y por semana, se cierra sin construir nada.

---

### 4.6 — Descartados de entrada (para no volver a proponerlos)

- **Libro de órdenes / microestructura:** compite en latencia contra infraestructura
  profesional. No es terreno para retail desde una PC.
- **On-chain:** señal lenta, ruidosa y ya arbitrada por quien tiene datos mejores.
- **Market making:** el maker spot en Binance VIP0 no tiene rebate; sin ventaja de fees
  no hay negocio.

---

## 5. Primeros pasos para la conversación nueva

1. Leer la sección 3 (las trampas). **Cinco de los seis hallazgos falsos de este repo
   salieron de ahí.**
2. Elegir **un** ítem de la sección 4 y releer su regla de parada antes de correr nada.
3. Correr la línea base de `banco/` para tener contra qué comparar.
4. Medir. Contrastar contra la regla escrita. Seguir o cerrar — sin renegociar.

**Prompt sugerido:**

> Leé `HANDOFF_SENALES.md` en la raíz. Arranquemos por el ítem 4.1 (fadear el propio
> screener): separar el efecto del score del efecto del tipo de señal en
> `daytrader_outcomes`, y contrastar contra la regla de parada escrita en ese ítem.

---

## 6. Expectativa honesta

Cuatro líneas medidas, cuatro cerradas. Eso es evidencia real de que **el rincón
explorado está vacío**: reglas de umbral sobre velas públicas, solo largo, horizontes de
minutos a 30 días. Pero es un rincón, no la habitación — lo de la sección 4 son familias
distintas, no variantes de lo mismo.

Lo más probable sigue siendo que ninguna cruce. Si eso pasa, el resultado no es el
fracaso: es haber comprado certeza barata. Cuatro proyectos cerrados en días, con la
regla escrita antes de mirar, es más disciplina de la que aplica la mayoría de la gente
que opera con plata real durante años.

**El piso de stablecoins no es el premio consuelo. Es el rival, y hasta hoy va ganando.**
