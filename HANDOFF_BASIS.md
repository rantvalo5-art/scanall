# HANDOFF — Basis / funding capture (proyecto nuevo)

> ⛔ **FASE 1 CORRIDA EL 2026-08-10 — LA REGLA DE PARADA DISPARÓ. PROYECTO CERRADO.**
> El resultado está en la sección 0. El resto del documento queda como estaba, para
> que se entienda qué se creía antes de medir. **No re-abrir sin datos nuevos.**

---

## 0. RESULTADO DE LA FASE 1 (2026-08-10)

Código: `basis/` (`fetch_funding.py`, `phase1.py`, `diag_fase1.py`, `sim_selectivo.py`).
Datos: funding real de **351 perps USDT con pata spot**, ventana **fija** 2025-08-01 →
2026-08-01, bajado de `fapi` desde la PC local. Salida cruda en `basis/fase1.json`.

### El supuesto que rompe todo

La sección 3 asumía funding neutral de 0,01% cada 8h = **11% anual**. Ese número es el
*default* de Binance, no lo que se cobra. Lo realizado en 12 meses:

| símbolo | APY bruto s/notional | | universo | APY bruto |
|---|---|---|---|---|
| BTCUSDT | **+3,35%** | | mediana de los 351 | **−4,71%** |
| ETHUSDT | +2,46% | | media | −17,17% |
| DOGEUSDT | +2,88% | | símbolos con funding **negativo** | **66%** |
| SOLUSDT | −1,62% | | p95 del trailing 3d anualizado | +8,56% |

BTC cobra **un tercio** de lo supuesto. Y hay un techo visible: el p95 de todo el panel
es +8,56% anual **sobre notional** — antes de dividir por 1,35x de capital y antes de
los costos. El piso de stablecoins (7%) está por encima de casi toda la distribución.

### La pregunta central sí tiene respuesta — y es que sí

*¿El funding alto dura más que los ~10 días de break-even?* **Sí.** Condicionando a
trailing 3d > 20% anual: el 80,7% cubre el round-trip en ≤10 días, mediana de
break-even **4 días** (vs 14 días incondicional). Con umbral >50%, sube a 89,2%.
El funding alto **persiste**. Eso no era obvio y era lo que había que medir.

### Pero muere en el chequeo de concentración

Califica el **0,36% de las oportunidades** (388 de 108.274), repartidas en 37 símbolos.
Simulando la cartera completa — capital ocioso rindiendo el piso, que es la única
comparación honesta — el mejor caso da +12,7% anual… y **ninguna configuración
sobrevive sacar el top-3**:

| umbral / tenencia | cartera | sin top-3 |
|---|---|---|
| >30% / 14d, 3 slots | +12,68% | **−16,07%** |
| >50% / 14d, 10 slots | +8,83% | **+4,14%** |
| >50% / 7d, 10 slots | +8,58% | **+5,17%** |
| >30% / 7d, 10 slots | +8,22% | **+2,49%** |

Ninguna versión sin top-3 llega al piso de 7%. Y el top-3 es siempre el mismo:
**币安人生USDT, BANKUSDT, GIGGLEUSDT** — listings meme ilíquidos. BANKUSDT es
*literalmente* el símbolo que ya dio vuelta cinco resultados del swing. El resultado
vive en tres monedas cuyo lado spot no tiene profundidad para operarlas en serio.

### Veredicto contra la regla de parada (sección 4)

| criterio | resultado |
|---|---|
| supera stablecoins con margen claro | ❌ buy&hold: mediana −3,71% |
| mediana positiva **por símbolo** | ❌ negativa en 7d/14d/30d |
| mediana positiva **por semana** | ❌ 0% de las 52 semanas supera el piso |
| sobrevive sacar el top-3 | ❌ **en ninguna configuración** |

**Se cierra.** La regla se escribió antes de ver los números, justamente para este
momento. Costó horas, no meses de pérdidas.

### Lo que queda sabido (no repetir el trabajo)

- El funding cobrable es estructuralmente **más chico** que el piso de stablecoins.
  No es cuestión de ajustar umbrales: el p95 del panel entero no llega.
- Régimen medido mes a mes: mediana entre símbolos va de **+8,12% (2025-09)** a
  **−4,05% (2026-02)**; jul-2026 fue +5,29%. Ni el mejor mes despeja el piso.
- La cola **negativa** es enorme (media −17% anual, p01 del trailing −335%). El trade
  espejo — long perp + short spot — cobraría eso, pero shortear spot exige pedir
  prestada la moneda y el interés de margen en alts supera al funding. No es una salida.
- **Sesgo de supervivencia declarado:** el universo son perps listados hoy; los
  delisted del período no están. Sesga hacia mejor, no hacia peor.

---

## 1. Por qué este proyecto existe

Vengo de medir el swinger (`swing/`) contra datos reales de jul-2026 y el veredicto es que
**no tiene ventaja**: sus alertas rinden −1,61% de exceso vs BTC a 7d y el universo entero
rinde −1,65%. Igual. El control de momento (misma moneda, momento al azar) rinde −0,79%,
o sea que el bot elige momentos *peores* que al azar.

La causa está identificada y no es de ajuste: **todo lo que el screener mide (ATR, volumen,
Bollinger, distancia al máximo) mide cuánto se mueve una moneda, no para qué lado.** Subir
el corte de score lleva P(+30%) de 2,0% a 4,9% **y** P(−30%) de 1,1% a 2,7% — el ratio queda
clavado en ~1,8, que es el que el universo regala gratis. Es un detector de volatilidad
excelente conectado a una estrategia que necesita dirección.

Se probaron y descartaron, con medición: apagar/demorar señales, filtrar por score o bucket,
144 combinaciones de salida, comprar el dip, concentrar en pocas alertas, y cobrar la
volatilidad con entradas a dos puntas en perps (falla estructuralmente: no se puede replicar
un straddle con órdenes stop, se regala k·ATR por trade).

**Conclusión: dejar de predecir dirección desde velas públicas.** Este proyecto va por una
ventaja que no requiere predecir nada.

---

## 2. La estrategia, en una línea

Comprar spot de una moneda y shortear su perp por el mismo notional. Queda **neutral a
precio** — no importa si sube o baja — y se cobra el *funding* que los longs apalancados
le pagan a los shorts.

## 3. La economía que decide todo

Este es el cálculo del que depende el proyecto entero. Hacerlo con números reales **antes
de escribir una línea de bot**.

**Ingreso.** Binance paga funding cada 8h (00:00 / 08:00 / 16:00 UTC). La tasa "neutral"
típica es 0,01% por período = **0,03%/día ≈ 11% anual**. Puede dispararse a 0,05-0,1% por
período en euforia, y puede ponerse **negativa** (ahí pagás vos).

**Costo.** Cuatro patas por operación completa, a tarifas VIP0:

| pata | taker | maker |
|---|---|---|
| comprar spot | 0,10% | 0,10% |
| shortear perp | 0,05% | 0,02% |
| vender spot | 0,10% | 0,10% |
| cerrar perp | 0,05% | 0,02% |
| **round-trip** | **0,30%** | **0,24%** |

**El número que importa:** a funding neutral (0,03%/día) hacen falta **~10 días de
posición solo para cubrir los costos**. Todo lo que se gane sale de después del día 10.

De ahí salen las dos únicas formas de que esto funcione: **(a)** mantener posiciones largas
y rotar poco, o **(b)** seleccionar monedas/momentos donde el funding esté muy por encima
del neutral. La Fase 1 existe para determinar cuál de las dos (o ninguna) se sostiene.

## 4. Regla de parada — pre-registrada, escribirla antes de medir

Este proyecto compite contra **prestar USDT en Binance Earn (~5-10% anual, sin riesgo de
liquidación y sin operar)**. Ese es el piso real, no el cero.

> **Se sigue solo si:** el retorno neto de costos supera el rendimiento de stablecoins por
> un margen claro, **con mediana positiva** por símbolo y por semana, **y** sobrevive sacar
> los 3 símbolos que más aportan. Si el resultado vive en una moneda o en una semana, se
> descarta y se cierra el proyecto.

Este criterio no se negocia después de ver los números. Si no pasa, se cierra — y eso
también es un resultado que valió la pena.

---

## 5. Fases

| # | Fase | Capital | Salida esperada |
|---|---|---|---|
| 1 | **Medir** funding histórico vs costos | $0 | ¿pasa la regla de parada? |
| 2 | **Paper**: el bot calcula y loguea lo que haría, sin órdenes | $0 | ¿la ejecución coincide con lo medido? |
| 3 | **Vivo chico**: 1-2 símbolos, tamaño que no duela | mínimo | ¿slippage y funding reales coinciden? |
| 4 | Escalar | — | — |

**No saltear la Fase 1.** Todo el historial de este repo es de construir antes de medir.

### Qué mide exactamente la Fase 1

1. Bajar funding histórico de todos los perps USDT (ver fuentes abajo), 6-12 meses.
2. Para cada símbolo, calcular el funding acumulado en ventanas de N días (N = 7, 14, 30).
3. Restar costos (0,30% round-trip) y calcular retorno neto anualizado por ventana.
4. Cortar por símbolo, por semana y por régimen de mercado.
5. Responder:
   - ¿Qué fracción de (símbolo, ventana) supera el piso de stablecoins **neto**?
   - ¿El funding alto **persiste** lo suficiente para cobrarlo, o se apaga antes de los
     10 días de break-even? ← *pregunta central; si el funding alto es efímero, el
     proyecto muere acá*
   - ¿Cuánto pierde una posición cuando el funding se da vuelta?
   - ¿Alcanza la liquidez del spot para entrar y salir del tamaño que pensás operar?

---

## 6. Restricciones operativas ya conocidas

**`fapi.binance.com` está geo-bloqueado desde runners de GitHub y desde Cloudflare** (451 /
403 CloudFront). Está documentado en `swing/backfill_funding.py`. Consecuencias:

- **GitHub Actions NO sirve para este bot.** Ni para datos live ni para operar. El swing lo
  esquivaba usando `data.binance.vision` (dumps mensuales, sin geo-block) pero eso solo
  sirve para histórico, no para ejecutar.
- Necesita correr en **una máquina propia o un VPS en jurisdicción permitida**. Definir esto
  antes de la Fase 2.
- Desde esta PC (Windows local) `fapi` **sí responde** — verificado el 2026-08-09. Sirve
  para la Fase 1.

**Otras:**
- Se necesitan **las dos patas**: no todo perp tiene par spot y viceversa. Del universo spot
  analizado (444), 354 tienen perp (80%).
- **Riesgo de liquidación en la pata corta**: si el precio sube mucho, el short pierde. Con
  spot comprado el conjunto está cubierto, pero el margen del perp puede liquidar antes.
  Usar margen cruzado o apalancamiento muy bajo y dejar colchón.
- Riesgo de exchange (todo el capital en un solo lugar, en dos productos distintos).
- El funding se puede dar vuelta: hace falta una regla de salida definida por adelantado.

---

## 7. Relación con lo que ya existe (no mezclar)

El monorepo tiene dos proyectos y este sería el tercero:

- **raíz** (`screener.py`, `backtest.py`, `config.json`) — day trader, 5m/15m/1h, CON derivados
- **`swing/`** — swinger, 1h/4h/1d, señales PREBREAK/BREAKOUT/HOLD/COILING
- **este** — nuevo, delta-neutral, sin predicción. **Carpeta propia, branch propio, config propio.**

### Piezas reutilizables (rutas verificadas el 2026-08-09)

| pieza | dónde | para qué |
|---|---|---|
| descarga de funding histórico | `backtest.py:384` (`/fapi/v1/fundingRate`), `backtest.py:401` (`download_all_derivatives`) | Fase 1 |
| funding vía dumps sin geo-block | `swing/backfill_funding.py` | alternativa si `fapi` falla |
| open interest | `screener.py:432`, `backtest.py:359` | contexto, opcional |
| costos como config | `swing/backtest.py` → `round_trip_cost()` y sección `costs` | copiar el patrón |
| chequeos de robustez | `swing/backtest.py` → `summarize_tails()`, `tail_rates()`, `universe_tail_baseline()` | copiar la **disciplina**, no el contenido |
| persistencia / alertas | Supabase + Telegram ya cableados | Fases 2-3 |

### Lo que hay que traerse sí o sí: el marco de medición

Es lo más valioso que dejó el swing. Sin esto, cinco hallazgos falsos ya se dieron por
buenos en este repo:

1. **Mediana además de media.** La media siempre está cargada por una cola derecha corta.
2. **Chequeo de concentración.** Recalcular sacando el top-1, top-3 y top-5 aportantes. En
   el swing, BANKUSDT (+672% en julio) dio vuelta cinco resultados distintos.
3. **Partir por semana.** Si el resultado vive en una sola, es ruido.
4. **Costos desde el día uno.** Antes de agosto ningún número del repo los incluía.
5. **Ventana fija.** `--weeks` es relativo a hoy: dos corridas separadas cubren períodos
   distintos y eso solo ya fabricó artefactos de varios puntos porcentuales. Usar siempre
   fecha de corte explícita y comparar variantes en una sola corrida.
6. **Regla de parada escrita antes de correr.** Si un experimento no puede cambiar la
   decisión, no correrlo.

---

## 8. Primeros pasos para la conversación nueva

1. Confirmar dónde va a correr esto (PC local / VPS) — condiciona todo lo demás.
2. Bajar funding histórico de los perps USDT, 6-12 meses, y cachearlo en disco.
3. Correr el análisis de la Fase 1 y contestar la pregunta central: **¿el funding alto dura
   más que los ~10 días de break-even?**
4. Contrastar contra la regla de parada de la sección 4. Seguir o cerrar.

**Prompt sugerido para arrancar:**

> Leé `HANDOFF_BASIS.md` en la raíz del repo. Arranquemos por la Fase 1: bajar funding
> histórico de los perps USDT de Binance y medir si el carry neto de costos supera el
> rendimiento de stablecoins, aplicando la regla de parada de la sección 4.

---

## 9. Expectativa honesta

Esto rinde **poco y aburrido**: el techo realista es dígito simple a medio-adolescente
anual, y el piso contra el que compite (prestar stablecoins) ya da 5-10% sin riesgo de
liquidación. Es muy posible que la Fase 1 concluya que **el margen sobre ese piso no
justifica el riesgo operativo** — y ese es un resultado válido que se descubre en días, no
en meses de pérdidas.

Lo que sí tiene, y el swing nunca tuvo: **la ventaja es estructural, no predictiva.** El
funding lo pagan longs apalancados porque quieren apalancamiento, no porque se equivoquen
de dirección. Eso no se arbitra hasta desaparecer del mismo modo que un patrón de velas.
