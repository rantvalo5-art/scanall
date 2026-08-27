# HANDOFF — leer el forward test del radar

> Escrito el **2026-08-27**, el día que el radar empezó a correr. Se puede abrir en frío:
> la sección 1 tiene lo operativo y no hace falta saber nada de antes.
>
> **No hay una fecha única.** La primera versión de este archivo decía "esperá 8 semanas",
> heredado de `banco/lote.py` — donde ese umbral existía porque allá las entradas SE
> SOLAPAN. Acá no se solapan por diseño. `banco/cuanto_esperar.py` lo calculó de verdad
> y el calendario es escalonado (sección 3.5).
>
> **La decisión ya está tomada.** Este archivo no es para decidir en octubre — es para
> ejecutar lo que se decidió hoy, cuando todavía no se sabía el resultado. Si algo acá
> se afloja después de ver un número, el experimento no valió nada.

---

## 0. Qué es esto en dos párrafos

`radar/radar.py` ordena ~78 monedas por **actividad** (operaciones de la última hora
contra su mediana de 7 días) y devuelve las 8 que más probablemente **se muevan** en las
próximas 4 horas. **No dice para dónde.** Corre solo cada 4h por GitHub Actions y guarda
el universo entero en la tabla `radar_runs` de Supabase.

Se construyó así porque se midieron **4.140 formas de predecir dirección** —precio, flujo
de órdenes y posicionamiento de futuros, las dos direcciones, de 4h a 7 días, 5 años,
4 regímenes— y **sobrevivieron cero**. Predecir *magnitud* sí funcionó: 38 rankings
sobreviven y aguantan los cuatro regímenes. El detalle está en
`banco/PREREGISTRO_TRANSVERSAL.md` (rama `banco/primer-toque`).

---

## 1. Qué correr

```powershell
$env:PYTHONIOENCODING = "utf-8"
$env:SUPABASE_KEY = "<anon key>"
cd C:\Users\asd\Pictures\scanall\radar
py -3.13 -u medir.py
```

**Gotchas del repo, todos pisados:** siempre `py -3.13` (nunca `python`), siempre `-u`, y
`PYTHONIOENCODING=utf-8` o revienta con cp1252.

La anon key está en
`~/.claude/projects/C--Users-asd-scancrypto-scanall/memory/reference-supabase-anon-key.md`.
Se puede cargar sin imprimirla:

```powershell
$env:SUPABASE_KEY = (Select-String -Path "$env:USERPROFILE\.claude\projects\C--Users-asd-scancrypto-scanall\memory\reference-supabase-anon-key.md" -Pattern 'eyJ[A-Za-z0-9_.\-]+' -AllMatches).Matches[0].Value
```

---

## 2. ANTES de mirar el resultado: ¿los datos son válidos?

No tiene sentido interpretar un número si la recolección se rompió. Correr esto primero,
en el SQL Editor de Supabase:

```sql
select date_trunc('day', run_at) dia, count(distinct run_at) corridas, count(*) filas
from radar_runs group by 1 order by 1;
```

**Tiene que dar ~6 corridas por día y ~78 filas por corrida.** Qué mirar:

| síntoma | qué pasó | qué hacer |
|---|---|---|
| días con 0 corridas | GitHub saltó el cron (pasa en horas pico) | si son pocos, seguir; el de-solape y el conteo por semana lo absorben |
| **se corta todo de golpe** | **GitHub deshabilita los cron tras 60 días sin actividad en el repo** | reactivarlo en Actions; el MDE que calcula `medir.py` se ajusta solo al n real |
| menos de 30 filas por corrida | falló la bajada de klines, no hay sección cruzada | mirar el log de esa corrida en Actions |
| `oi_rel_168` siempre nulo | **esperado.** `fapi.binance.com` está geo-bloqueado desde los runners y no tiene mirror | ignorar: es informativo, `medir.py` no lo usa |

`medir.py` además **de-solapa solo**: descarta corridas separadas por menos de 4h de la
anterior e informa cuántas. Las 3 primeras corridas del 2026-08-27 son pruebas y van a
colapsar a una — está bien.

---

## 3. Cómo leer la salida

```
                        medido antes       EN VIVO
================================================================
spread (ATR base)             +0.511        +0.???
multiplo de camino             1.21x         ?.??x
tasa de acierto                62.6%         ??.?%
linea base                     49.5%         ??.?%
semanas                          251            ??
```

- **spread** es el estadístico que decide. Es "las 8 elegidas menos el universo de la
  misma barra", en unidades del ATR base de cada moneda.
- **múltiplo** y **tasa** son la traducción a algo que se siente: cuánto más recorren y
  cuántas veces le pega.
- **línea base 49,5%** es lo que da tirar un dado. Si la tasa en vivo se le pega, no hay
  señal.

---

## 4. Cuándo mirar — el calendario, calculado

`banco/cuanto_esperar.py` midió la autocorrelación real del spread por barra (**+0,449**
entre barras consecutivas, factor de inflación de varianza **4,24×**) y de ahí sale
cuántos datos hacen falta según cuán grande sea el efecto de verdad:

| si el efecto real es | días | fecha aproximada |
|---|---|---|
| **lo medido (×1,0)** | **12** | **2026-09-08** |
| la mitad (×0,5) | 48 | 2026-10-14 |
| un tercio (×0,33) | 107 | 2026-12-12 |
| un cuarto (×0,25) | 191 | 2027-03-06 |

**Refutar es mucho más rápido que confirmar.** Si el efecto es tan grande como se midió,
se ve a los 12 días. Por eso conviene mirar temprano y seguido en vez de esperar sentado:

- **~8 de septiembre** — primer chequeo. Si replica con el tamaño completo, ya se ve. Si
  sale claramente negativo, ya se puede matar. Si cae dentro del MDE, no dice nada.
- **~14 de octubre** — el chequeo que decide para un efecto de la mitad del tamaño, que
  es lo más probable.
- **~12 de diciembre** — solo si a esa altura sigue dentro del MDE y querés agotar la
  pregunta.

`medir.py` calcula el MDE **en cada corrida** con los datos que hay, así que no hace falta
seguir el calendario a rajatabla: corrélo cuando quieras y él te dice si ya alcanza.

---

## 5. La decisión — fijada el 2026-08-27, antes de que existiera un solo dato

### Caso A — el observado cae DENTRO del MDE

`medir.py` lo dice así: *"TODAVIA NO ALCANZA"*, y te calcula cuántos días faltan para
decidir sobre un efecto de ese tamaño. **No interpretar nada todavía** — y sobre todo:
esto **no es "no está"**, es "no se pudo medir". La diferencia importa.

### Caso B — spread NEGATIVO y fuera del MDE

**No replicó. Se apaga.**

```powershell
# comentar el bloque `schedule:` en .github/workflows/radar.yml y commitear
```

No es un fracaso: es el sistema funcionando. Significa que el +1,21× era del período
medido y no del mundo. Lo que queda abierto está en la sección 7.

**Prohibido**: cambiar `n_surge` por otra feature, mover `k`, o mover el horizonte "a ver
si con eso sí". Eso convierte el out-of-sample en in-sample y no queda ninguna ventana
limpia para volver a preguntar. Si se quiere probar otra cosa, se preregistra y se junta
una ventana nueva.

### Caso C — spread positivo y por encima del MDE

**Replicó.** Es el primer resultado de este repo que sobrevive un forward test de verdad.
`medir.py` te dice además qué porcentaje del tamaño preregistrado alcanzó — si es el 50%,
es lo normal y sigue siendo una réplica.

Acción: **no tocar nada del radar.** Lo que se abre es una pregunta distinta, la de la
sección 6.

---

## 6. Si replica: la pregunta que sigue, y no es "tunearlo"

Saber que algo se va a mover **no es plata**. Para cobrar movimiento sin saber la
dirección hace falta un instrumento convexo, y en cripto eso prácticamente solo existe
para BTC y ETH. Sintetizarlo con órdenes stop ya se midió y no funciona: se regala k·ATR
por trade (`[[project-dos-puntas-descartado]]`).

Las tres vías, en orden de lo que yo probaría:

1. **Usarlo como radar y nada más.** Te dice dónde mirar. La decisión de qué hacer sigue
   siendo tuya y no hay nada medido que la respalde — pero tampoco hay nada que la
   contradiga.
2. **Opciones de alts.** Ítem 4.4 de `HANDOFF_SIGUIENTE.md`. El primer paso es de
   viabilidad, no de estadística: averiguar si existe algún mercado de opciones de alts
   con volumen real (OKX, Bybit). **Si no hay instrumento, se cierra ahí.**
3. **Volver a la dirección, pero con otra información.** Ver sección 6.

---

## 7. Lo que quedó sin probar (para la dirección)

La dirección dio 0 de 4.140 **en un diseño**: ranking transversal top-k, sobre precio,
flujo del kline y posicionamiento de futuros, de 4h a 7 días. Sigue sin tocarse:

| qué | dónde está | por qué puede valer |
|---|---|---|
| **velas de 5m** | 200 pares ya cacheados en `banco/.kline_cache/` | la única resolución de flujo sin mirar; el desbalance agresor tiene más señal intra-hora |
| **el libro de órdenes** | `banco/libro.py`, ya mide spread y profundidad | medido pero **nunca rankeado** |
| **formas distintas** | — | banda (ni alto ni bajo), Δrank (cambio de posición), multi-feature |
| listados nuevos / on-chain | sin datos | el n va a ser chico: contar el n post-join y el MDE **antes** de escribir la regla |

---

## 8. Las reglas de método que no se negocian

> **La regla de parada se escribe ANTES de mirar.** Si se afloja después de ver un
> número, el experimento no vale.

> **El n efectivo son las SEMANAS, no las corridas.**

> **Apretar una compuerta después de mirar está permitido; aflojarla no.** Apretar solo
> puede matar hallazgos propios; aflojar fabrica falsos positivos.

> **"SOBREVIVE" no es "funciona".** Es "no lo pude matar en esta ventana".

---

## 9. Dónde está cada cosa

| | |
|---|---|
| `radar/radar.py` | el screener. 240 líneas, cero configuración |
| `radar/medir.py` | el forward test, con la regla de parada cableada |
| `radar/tabla.sql` | la tabla de Supabase (ya creada) |
| `.github/workflows/radar.yml` | el cron cada 4h |
| `banco/PREREGISTRO_TRANSVERSAL.md` | las 4 corridas que llevaron acá — **rama `banco/primer-toque`, sin mergear** |
| `banco/ranking.py` | el harness de rankings transversales, reusable |

⚠️ **La rama `banco/primer-toque` arrastra 3 commits que cambian el swing en producción**
(`swing/backtest.py` +753 líneas, `screener.py`, `config.json`). Si se mergea, se revisan
esos aparte — es una decisión sobre el swing, no sobre el radar.

---

## 10. Lo honesto

El piso de stablecoins (5-10% anual, sin drawdown) sigue siendo el rival y sigue ganando.
Esperar los dos meses no es tiempo muerto: es la opción que ya va adelante mientras la
otra se prueba.

Y el resultado más probable es que a las pocas semanas siga **dentro del MDE**, o que replique con la mitad del tamaño medido. Eso no es una decepción — es lo que le pasa a casi todo lo que se mide honestamente.
