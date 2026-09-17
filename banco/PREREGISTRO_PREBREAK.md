# PREREGISTRO — aflojar PREBREAK

> Escrito el **2026-08-27 DESPUES de ver un screen de 4 semanas.** Esto NO es ciego y hay
> que tratarlo como tal: la variante P5 se eligio porque su screen se veia bien. Todo lo
> que sigue es confirmatorio, no exploratorio, y la barra tiene que ser mas alta.

## De donde sale

Seccion 9 de `PREREGISTRO_RANKING.md`: el precio sube +3,12 ATR en las 24h ANTES de la
alerta y baja −0,94 despues. El defecto es comun a todas las senales **menos PREBREAK**,
que es la unica que no viene precedida de corrida (+0,89) y la unica con camino posterior
plano (−0,03 a 48h). Pero es el 3,3% de las alertas (89 de 2.711).

Pregunta: **¿se puede subir el volumen de PREBREAK sin contaminarlo con el defecto?**

## La variante

`config_P5_todo.json` afloja las cuatro compuertas a la vez:

| perilla | base | P5 |
|---|---|---|
| PREBREAK_NEAR_MAX | 0,008 | 0,020 |
| PREBREAK_MIN_VOL_RATIO | 3,0 | 2,0 |
| PREBREAK_BB_WIDTH_MAX | 0,025 | 0,040 |
| PREBREAK_VOLUME_GROWTH_MIN | 1,2 | 1,0 |

## El screen que ya se vio (4 semanas, 2026-07-04 → 2026-07-31)

PREBREAK 11 → **106**. Subida previa **−1,06** (sin corrida previa). Camino a +48h
**+0,37**. Contra BREAKOUT +3,32/−1,28, HOLD +5,10/−1,03, RIDING +3,53/−1,55.

**Ojo con un efecto que el screen ya muestra**: PREBREAK no suma alertas nuevas nada mas,
tambien **roba** de los otros tipos (HOLD 106→98, RIDING 103→85, BREAKOUT 73→61). El
motor elige UNA senal por simbolo y PREBREAK tiene prioridad 1. O sea que parte del
efecto puede ser "las mismas oportunidades, detectadas antes" — que seria justamente lo
que se busca — pero hay que medirlo, no asumirlo.

## Metrica y compuertas

Primaria: **mediana del retorno a 24h normalizada por `atr_24`**, con las compuertas de
`gate_mediana.py`: diferencia pareada dentro de la semana, bootstrap de semanas,
concentracion top-1 y top-3, consistencia semanal ≥60%, FDR.

Secundaria y obligatoria: **subida previa ≤ 0**. Si al aflojar PREBREAK empieza a
comprar techo, el volumen no sirve por bueno que se vea el retorno.

## Regla de parada — fijada ANTES de la corrida de 28 semanas

1. **Si la subida previa de PREBREAK-P5 se vuelve positiva** (empieza a comprar corrida)
   → contaminada. Muere, sin importar el retorno.
2. **Si la mediana normalizada no supera al dardo pareado con p_bloques < 0,10 y ≥60% de
   semanas** → muere.
3. **Si muere a costo 0,50% aunque viva a 0,20%** → no cuenta.
4. **Si sobrevive**: NO se cree todavia. Como este preregistro NO es ciego, la unica
   evidencia que vale es la **reserva OOS de 2026-08 en adelante**, que sigue sin tocar,
   mas el dump de vivo. Recien ahi se decide si se toca `config.json`.
5. **Barrido de `n_min` obligatorio** (20 → 1). Si el veredicto depende de donde se pone
   el filtro de actividad semanal, es el filtro y no la senal. Ya paso dos veces.

Lo que **no** cuenta: que el camino "se vea" plano, que el volumen suba, o que la media
sea positiva (la secuestra un par).

---

# RESULTADOS

## In-sample (28 semanas, 900 PREBREAK) — pasa las cuatro compuertas

| compuerta | resultado |
|---|---|
| 1. subida previa ≤ 0 | **−0,73** limpia (resto +1,86 a +3,94) |
| 2. mediana vs dardo pareado | **+0,54**, sin top-3 +0,58, 86% semanas, p 0,0070 |
| 3. costo 0,50% | idéntico: +0,54, p 0,0070 |
| 5. barrido n_min 20→1 | +0,37 / +0,50 / +0,37 / +0,36 / +0,36 / +0,37 — **estable** |

Volumen PREBREAK 89 → 921 (×10,3). Las cuatro señales de referencia fallan
(BREAKOUT −0,45, HOLD −0,41, RIDING −0,22, EXPLOSION 0,00).

**Correccion de un stat mio**: reporte un "adelanto de −6,8h" de P5 sobre la alerta base.
Era artefacto: `.min()` sobre el delta con signo elige siempre la contraparte de −24h.
Con la contraparte MAS CERCANA el numero real es **mediana +0,0h, 41% antes / 40%
despues**: **no hay adelanto sistematico**. El valor de P5 esta en que el **56%** de sus
PREBREAK son oportunidades que el base nunca alerto, no en anticipar las que ya tenia.

## Reserva OOS (2026-08-01 → 2026-08-26, 5 semanas, 207 PREBREAK) — MUERE

| | in-sample | OOS |
|---|---|---|
| **vs dardo pareado** | **+0,54** | **−0,108** |
| vs linea base | +0,61 | +0,344 |
| semanas > 0 | 86% | 80% |
| **subida previa** | **−0,73 (limpia)** | **+0,31 (contaminada)** |

**Disparan las reglas 1 y 2 del preregistro, las dos:**

1. La subida previa se volvio **positiva** (+0,31). Empezo a comprar corrida. Sigue
   siendo mucho menor que BREAKOUT +3,61 o HOLD +4,70 —o sea PREBREAK sigue siendo
   estructuralmente distinta— pero cruzo el umbral que se habia fijado.
2. **No supera al dardo pareado**: +0,54 in-sample → **−0,108** OOS. Cambio de signo en
   el estadistico que decide.

**Veredicto: MUERE.**

## Lo que NO se puede hacer, y por que se anota

OOS `vs linea base` da +0,344 con 80% de semanas, que se ve bien. **Cambiar ahora el
criterio de `vs dardo` a `vs linea base` seria exactamente "aflojar una compuerta y
volver a mirar"**, que es como se fabrica un falso positivo. El criterio se fijo antes:
el dardo pareado es el control que aisla el MOMENTO de la eleccion de moneda, y es el que
importa porque la seccion 9 mostro que la eleccion de moneda aporta cero.

Lo que dice el par (+0,344 vs base, −0,108 vs dardo) es coherente y poco halagueño: P5
PREBREAK **elige monedas mejores que el resto de las alertas, pero su momento no le gana
a entrar al azar en esas mismas monedas**. Misma enfermedad que todo lo demas, mas leve.

## Limitaciones honestas del OOS

- **5 semanas, n=207.** El bootstrap de bloques pide ≥8 semanas, asi que el OOS verifica
  direccion y magnitud pero **no da p gateado**. Un OOS mas largo podria dar vuelta esto
  en cualquier direccion.
- El base solo emitio **15** PREBREAK en la ventana OOS, muy pocas para comparar P5
  contra el PREBREAK original.
- Este preregistro **no era ciego** (P5 se eligio viendo su screen), que es justamente por
  lo que la barra del OOS estaba puesta alta.

## Que quedaria por probar

El unico angulo que este resultado no cierra: la contaminacion aparecio al aflojar **las
cuatro** perillas juntas. `config_P1..P4` estan generadas y sin correr. Si alguna sola
—probablemente `P1_nearmax`, que es la que gobierna cuan lejos del maximo se permite
entrar— sube volumen sin cruzar la compuerta de subida previa, seria un test distinto y
todavia no hecho. No es continuar mirando lo mismo: es otra variante, con la misma regla.
