-- radar_runs — una fila por (corrida, simbolo), con el UNIVERSO ENTERO.
--
-- Se guardan las ~200 filas de cada corrida y no solo el top-8. No es prolijidad:
-- el estadistico validado es `media(top-k) - media(UNIVERSO DE LA MISMA BARRA)`.
-- Sin las filas del universo el forward test no se puede calcular, y no se pueden
-- agregar despues, porque el universo de hoy no es el de manana.
--
-- Pegar esto en el SQL Editor de Supabase.

create table if not exists radar_runs (
  id          bigserial primary key,
  run_at      timestamptz not null,
  symbol      text        not null,
  rank        int         not null,   -- 1 = mas actividad relativa
  en_top      boolean     not null,   -- entro al top-k reportado
  universo    int         not null,   -- cuantos pares habia en la seccion cruzada
  n_surge     double precision,       -- EL EJE: operaciones 1h / mediana 168h
  turnover    double precision,       -- volumen USD 1h / mediana 168h
  atr_base    double precision,       -- rango horario tipico (mediana movil 30d)
  precio      double precision,       -- cierre de la ultima vela CERRADA = referencia
  oi_rel_168  double precision        -- solo se completa para el top-k
);

create index if not exists radar_runs_run_at_idx  on radar_runs (run_at);
create index if not exists radar_runs_symbol_idx  on radar_runs (symbol, run_at);

-- Una sola fila por corrida y simbolo: si el cron se dispara dos veces (pasa), la
-- segunda no duplica.
create unique index if not exists radar_runs_uniq on radar_runs (run_at, symbol);

-- El screener escribe con la anon key, asi que hace falta la politica.
alter table radar_runs enable row level security;

drop policy if exists radar_runs_insert on radar_runs;
create policy radar_runs_insert on radar_runs
  for insert to anon with check (true);

drop policy if exists radar_runs_select on radar_runs;
create policy radar_runs_select on radar_runs
  for select to anon using (true);
