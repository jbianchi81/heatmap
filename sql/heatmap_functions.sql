CREATE OR REPLACE FUNCTION public.heatmap(_startdate date, _enddate date, _tabla text, _varid integer, _procid integer DEFAULT NULL::integer, _use_id text DEFAULT NULL::text, _has_obs boolean DEFAULT true)
 RETURNS TABLE(seriescontrol json)
 LANGUAGE plpgsql
AS $function$
 DECLARE
 _enddate2 timestamp := _enddate+'23:59:59'::interval;
 _procid int := case when _procid is not null then _procid when _varid = 4 then 2 else 1 end;
 BEGIN
RETURN QUERY WITH ts as (
    SELECT generate_series(_startdate::date,_enddate::date,'1 day'::interval) t
    ),
    tseries as (
    SELECT t,
           row_number() OVER (order by t) x
    FROM ts
    ),
    allstations as (
    SELECT estacion_id, 
           estaciones.id_externo,
           series.id series_id,
           estaciones.id id_interno,
           estaciones.nombre nombre,
           row_number() OVER (ORDER BY CASE WHEN (_use_id = 'ext' OR _use_id = 'ext_only') THEN id_externo WHEN _use_id = 'nombre' THEN nombre WHEN _use_id = 'id' THEN LPAD(estaciones.id::text,5,'0') ELSE LPAD(estacion_id::text,5,'0') END) y
    FROM series,estaciones
    WHERE estaciones.tabla=_tabla
    AND series.estacion_id=estaciones.unid
    AND series.var_id=_varid
    AND series.proc_id=_procid
    AND estaciones.habilitar = true
    AND estaciones.has_obs = CASE WHEN _has_obs = true 
								  THEN true
								  ELSE has_obs
							 END
    ORDER BY CASE WHEN (_use_id = 'ext' OR _use_id = 'ext_only')
                  THEN estaciones.id_externo
                  WHEN _use_id = 'nombre' 
                  THEN nombre 
                  WHEN _use_id = 'id' 
                  THEN LPAD(estaciones.id::text,5,'0')
                  ELSE LPAD(unid::text,5,'0')
             END
    ),
    subobs as (
		SELECT observaciones.*,
		       allstations.estacion_id,
		       allstations.nombre
		FROM observaciones,
			 allstations
		WHERE observaciones.series_id=allstations.series_id
		AND observaciones.timestart>=_startdate
		AND observaciones.timestart<=_enddate2
	),
    countreg as (
    SELECT subobs.estacion_id,
           subobs.timestart::date date,
           count(subobs.timestart) count 
    FROM subobs,
         tseries
    WHERE subobs.timestart::date=tseries.t
    GROUP BY subobs.estacion_id,
             subobs.timestart::date 
    ORDER BY subobs.estacion_id,
             subobs.timestart::date
    ),
        heatmap as (
    SELECT tseries.x, 
           allstations.y, 
           coalesce(countreg.count, 0) count
    FROM tseries
    JOIN allstations ON (allstations.estacion_id is not null)
    LEFT JOIN countreg ON (tseries.t=countreg.date AND allstations.estacion_id=countreg.estacion_id) 
    ),
    datearr as (
		SELECT array_agg(tseries.t::date) dates
		FROM tseries),
	starr as (
		SELECT CASE WHEN _use_id = 'ext'
		            THEN array_agg(substring(coalesce(allstations.id_externo,allstations.estacion_id::text),0,9) || ' - ' || substring(allstations.nombre,0,12)) 
		            WHEN _use_id = 'ext_only'
		            THEN array_agg(coalesce(allstations.id_externo,allstations.estacion_id::text)) 
		            WHEN _use_id = 'nombre'
		            THEN array_agg(substring(coalesce(allstations.nombre,allstations.estacion_id::text),0,16))
		            WHEN _use_id = 'id'
		            THEN array_agg(substring(coalesce(allstations.id_interno::text,allstations.estacion_id::text),0,16))
		            ELSE array_agg(substring(allstations.nombre,0,12) ||  ' (' || allstations.estacion_id || ')')
		       END estaciones
		FROM allstations
	), heatmaparr as (
       SELECT array_agg(ARRAY[heatmap.x::int-1, heatmap.y::int-1, heatmap.count::int]) heatmap
       FROM heatmap)
    SELECT json_build_object('dates',dates,'estaciones',estaciones,'heatmap',heatmap)
    FROM datearr,
         starr,
         heatmaparr;
END;
$function$

;
CREATE OR REPLACE FUNCTION public.heatmap_anio(_year integer, _tabla text, _varid integer, _procid integer DEFAULT NULL::integer, _use_id text DEFAULT false, _has_obs boolean DEFAULT true)
 RETURNS TABLE(seriescontrol json)
 LANGUAGE plpgsql
AS $function$
 DECLARE
 _startdate timestamp := make_timestamp(_year,1,1,0,0,0);
 _enddate timestamp := make_timestamp(_year,12,31,23,59,59);
 _procid int := case when _procid is not null then _procid when _varid = 4 then 2 else 1 end;
 BEGIN
RETURN QUERY WITH ts as (
    SELECT generate_series(_startdate::date,_enddate::date,'1 month'::interval) t
    ),
    tseries as (
    SELECT t,
           row_number() OVER (order by t) x,
           extract(month from t) mes
    FROM ts
    ),
    allstations as (
    SELECT estacion_id, 
           estaciones.id_externo,
           estaciones.id id_interno,
           series.id series_id,
           estaciones.nombre nombre,
           row_number() OVER (ORDER BY CASE WHEN (_use_id = 'ext' OR _use_id = 'ext_only') THEN id_externo WHEN _use_id = 'nombre' THEN nombre WHEN _use_id = 'id' THEN LPAD(estaciones.id::text,5,'0') ELSE LPAD(estacion_id::text,5,'0') END) y
    FROM series,estaciones
    WHERE estaciones.tabla=_tabla
    AND series.estacion_id=estaciones.unid
    AND series.var_id=_varid
    AND series.proc_id=_procid
    AND estaciones.habilitar = true
    AND estaciones.has_obs = CASE WHEN _has_obs = true 
								  THEN true
								  ELSE has_obs
							 END
    ORDER BY CASE WHEN (_use_id = 'ext' OR _use_id = 'ext_only')
                  THEN estaciones.id_externo
                  WHEN _use_id = 'nombre' 
                  THEN nombre 
                  WHEN _use_id = 'id' 
                  THEN LPAD(estaciones.id::text,5,'0')
                  ELSE LPAD(unid::text,5,'0')
             END
    ),
    subobs as (
		SELECT observaciones.*,
			   extract(month from observaciones.timestart) mes,
		       allstations.estacion_id,
		       allstations.nombre
		FROM observaciones,
			 allstations
		WHERE observaciones.series_id=allstations.series_id
		AND observaciones.timestart>=_startdate
		AND observaciones.timestart<=_enddate
	),
    countreg as (
    SELECT subobs.estacion_id,
           tseries.t date,
           count(subobs.timestart) count 
    FROM subobs,
         tseries
    WHERE subobs.mes=tseries.mes
    GROUP BY subobs.estacion_id,
             tseries.t 
    ORDER BY subobs.estacion_id,
             tseries.t
    ),
        heatmap as (
    SELECT tseries.x, 
           allstations.y, 
           coalesce(countreg.count, 0) count
    FROM tseries
    JOIN allstations ON (allstations.estacion_id is not null)
    LEFT JOIN countreg ON (tseries.t=countreg.date AND allstations.estacion_id=countreg.estacion_id) 
    ),
    datearr as (
		SELECT array_agg(tseries.t::date) dates
		FROM tseries),
	starr as (
		SELECT CASE WHEN _use_id = 'ext'
		            THEN array_agg(substring(coalesce(allstations.id_externo,allstations.estacion_id::text),0,9) || ' - ' || substring(allstations.nombre,0,12)) 
		            WHEN _use_id = 'ext_only'
		            THEN array_agg(coalesce(allstations.id_externo,allstations.estacion_id::text)) 
		            WHEN _use_id = 'nombre'
		            THEN array_agg(substring(coalesce(allstations.nombre,allstations.estacion_id::text),0,16))
		            WHEN _use_id = 'id'
		            THEN array_agg(substring(coalesce(allstations.id_interno::text,allstations.estacion_id::text),0,16))
		            ELSE array_agg(substring(allstations.nombre,0,12) ||  ' (' || allstations.estacion_id || ')')
		       END estaciones
		FROM allstations
	), heatmaparr as (
       SELECT array_agg(ARRAY[heatmap.x::int-1, heatmap.y::int-1, heatmap.count::int]) heatmap
       FROM heatmap)
    SELECT json_build_object('dates',dates,'estaciones',estaciones,'heatmap',heatmap)
    FROM datearr,
         starr,
         heatmaparr;
END;
$function$

;
CREATE OR REPLACE FUNCTION public.heatmap_areal(_startdate date, _enddate date, _fuenteid integer, _varid integer DEFAULT NULL::integer, _procid integer DEFAULT NULL::integer, _has_obs boolean DEFAULT true)
 RETURNS TABLE(seriescontrol json)
 LANGUAGE plpgsql
AS $function$
 DECLARE
 _enddate2 timestamp := _enddate+'23:59:59'::interval;
 BEGIN
RETURN QUERY WITH ts as (
    SELECT generate_series(_startdate::date,_enddate::date,'1 day'::interval) t
    ),
    tseries as (
    SELECT t,
           row_number() OVER (order by t) x
    FROM ts
    ),
    allareas as (
    SELECT series_areal.area_id, 
           series_areal.id series_id,
           areas_pluvio.nombre nombre,
           row_number() OVER (ORDER BY area_id) y
    FROM series_areal,areas_pluvio
    WHERE series_areal.area_id=areas_pluvio.unid
    AND series_areal.var_id= CASE WHEN _varid is not null THEN _varid ELSE series_areal.var_id END
    AND series_areal.proc_id= CASE WHEN _procid is not null THEN _procid ELSE series_areal.proc_id END
    AND series_areal.fuentes_id = _fuenteid
    AND areas_pluvio.activar = true
    AND areas_pluvio.mostrar = true
    ORDER BY unid
    ),
    subobs as (
		SELECT observaciones_areal.timestart,
		       allareas.area_id,
		       allareas.nombre
		FROM observaciones_areal,
			 allareas
		WHERE observaciones_areal.series_id=allareas.series_id
		AND observaciones_areal.timestart>=_startdate
		AND observaciones_areal.timestart<=_enddate2
	),
    countreg as (
    SELECT subobs.area_id,
           subobs.timestart::date date,
           count(subobs.timestart) count 
    FROM subobs,
         tseries
    WHERE subobs.timestart::date=tseries.t
    GROUP BY subobs.area_id,
             subobs.timestart::date 
    ORDER BY subobs.area_id,
             subobs.timestart::date
    ),
        heatmap as (
    SELECT tseries.x, 
           allareas.y, 
           coalesce(countreg.count, 0) count
    FROM tseries
    JOIN allareas ON (allareas.area_id is not null)
    LEFT JOIN countreg ON (tseries.t=countreg.date AND allareas.area_id=countreg.area_id) 
    ),
    datearr as (
		SELECT array_agg(tseries.t::date) dates
		FROM tseries),
	starr as (
		SELECT array_agg(substring(allareas.nombre,0,12) ||  ' (' || allareas.area_id || ')') areas
		FROM allareas
	), heatmaparr as (
       SELECT array_agg(ARRAY[heatmap.x::int-1, heatmap.y::int-1, heatmap.count::int]) heatmap
       FROM heatmap)
    SELECT json_build_object('dates',dates,'areas',areas,'heatmap',heatmap)
    FROM datearr,
         starr,
         heatmaparr;
END;
$function$

;
CREATE OR REPLACE FUNCTION public.heatmap_day(_date date, _dt interval, _tabla text, _varid integer, _procid integer DEFAULT NULL::integer, _use_id text DEFAULT NULL::text, _has_obs boolean DEFAULT true)
 RETURNS TABLE(seriescontrol json)
 LANGUAGE plpgsql
AS $function$
 DECLARE
 _startdate timestamp := _date::timestamp;
 _enddate timestamp := _date+'23:59:59'::interval;
 _procid int := case when _procid is not null then _procid when _varid = 4 then 2 else 1 end;
 BEGIN
RETURN QUERY WITH ts as (
    SELECT generate_series(_startdate,_enddate,_dt) t
    ),
    tseries as (
    SELECT t,
           row_number() OVER (order by t) x
    FROM ts
    ),
    allstations as (
    SELECT estacion_id, 
           estaciones.id_externo,
           series.id series_id,
           estaciones.id id_interno,
           estaciones.nombre nombre,
           row_number() OVER (ORDER BY CASE WHEN (_use_id = 'ext' OR _use_id = 'ext_only') THEN id_externo WHEN _use_id = 'nombre' THEN nombre WHEN _use_id = 'id' THEN LPAD(estaciones.id::text,5,'0') ELSE LPAD(estacion_id::text,5,'0') END) y
    FROM series,estaciones
    WHERE estaciones.tabla=_tabla
    AND series.estacion_id=estaciones.unid
    AND series.var_id=_varid
    AND series.proc_id=_procid
    AND estaciones.habilitar = true
    AND estaciones.has_obs = CASE WHEN _has_obs = true 
								  THEN true
								  ELSE has_obs
							 END
    ORDER BY CASE WHEN (_use_id = 'ext' OR _use_id = 'ext_only')
                  THEN estaciones.id_externo
                  WHEN _use_id = 'nombre' 
                  THEN nombre 
                  WHEN _use_id = 'id' 
                  THEN LPAD(estaciones.id::text,5,'0')
                  ELSE LPAD(unid::text,5,'0')
             END
    ),
    subobs as (
		SELECT observaciones.*,
		       allstations.estacion_id,
		       allstations.nombre
		FROM observaciones,
			 allstations
		WHERE observaciones.series_id=allstations.series_id
		AND observaciones.timestart>=_startdate
		AND observaciones.timestart<=_enddate
	),
    countreg as (
    SELECT subobs.estacion_id,
           tseries.t date,
           count(subobs.timestart) count 
    FROM subobs,
         tseries
    WHERE subobs.timestart >= tseries.t
    AND   subobs.timestart < tseries.t+_dt
    GROUP BY subobs.estacion_id,
             tseries.t 
    ORDER BY subobs.estacion_id,
             tseries.t
    ),
        heatmap as (
    SELECT tseries.x, 
           allstations.y, 
           coalesce(countreg.count, 0) count
    FROM tseries
    JOIN allstations ON (allstations.estacion_id is not null)
    LEFT JOIN countreg ON (tseries.t=countreg.date AND allstations.estacion_id=countreg.estacion_id) 
    ),
    datearr as (
		SELECT array_agg(tseries.t) dates
		FROM tseries),
	starr as (
		SELECT CASE WHEN _use_id = 'ext'
		            THEN array_agg(substring(coalesce(allstations.id_externo,allstations.estacion_id::text),0,9) || ' - ' || substring(allstations.nombre,0,12)) 
		            WHEN _use_id = 'ext_only'
		            THEN array_agg(coalesce(allstations.id_externo,allstations.estacion_id::text)) 
		            WHEN _use_id = 'nombre'
		            THEN array_agg(substring(coalesce(allstations.nombre,allstations.estacion_id::text),0,16))
		            WHEN _use_id = 'id'
		            THEN array_agg(substring(coalesce(allstations.id_interno::text,allstations.estacion_id::text),0,16))
		            ELSE array_agg(substring(allstations.nombre,0,12) ||  ' (' || allstations.estacion_id || ')')
		       END estaciones
		FROM allstations
	), heatmaparr as (
       SELECT array_agg(ARRAY[heatmap.x::int-1, heatmap.y::int-1, heatmap.count::int]) heatmap
       FROM heatmap)
    SELECT json_build_object('dates',dates,'estaciones',estaciones,'heatmap',heatmap) as heatmap
    FROM datearr,
         starr,
         heatmaparr;
END;
$function$

;


CREATE OR REPLACE FUNCTION public.heatmap_prono(_startdate date, _enddate date, _model_id int DEFAULT NULL,_use_id boolean DEFAULT TRUE)
 RETURNS TABLE(seriescontrol json)
 LANGUAGE plpgsql
AS $function$
 DECLARE
 _enddate2 timestamp := _enddate+'23:59:59'::interval;
 BEGIN
RETURN QUERY WITH ts as (
    SELECT generate_series(_startdate::date,_enddate::date,'1 day'::interval) t
    ),
    tseries as (
    SELECT t,
           row_number() OVER (order by t) x
    FROM ts
    ),
    distinct_cal AS (
       select distinct(cal_id) cal_id
       FROM corridas
    ),
    allcalibrados as (
    SELECT id, 
           nombre,
           model_id,
           row_number() OVER (ORDER BY CASE WHEN (_use_id = TRUE) THEN LPAD(calibrados.id::text,4,'0') ELSE LPAD(nombre::text,10,'0') END) y
    FROM calibrados, distinct_cal
    WHERE calibrados.id=distinct_cal.cal_id
    AND calibrados.model_id = CASE WHEN _model_id IS NOT NULL 
								  THEN _model_id
								  ELSE calibrados.model_id
							 END
    ORDER BY CASE WHEN (_use_id = TRUE)
                  THEN LPAD(calibrados.id::text,4,'0')
                  ELSE LPAD(nombre::text,10,'0')
             END
    ),
    subcorr as (
		SELECT corridas.cal_id,
                     corridas.date,
		       allcalibrados.nombre
		FROM corridas,
			 allcalibrados
		WHERE corridas.cal_id=allcalibrados.id
		AND corridas.date>=_startdate
		AND corridas.date<=_enddate2
	),
    countreg as (
    SELECT subcorr.cal_id AS id,
           subcorr.date,
           count(subcorr.date) count 
    FROM subcorr,
         tseries
    WHERE subcorr.date=tseries.t
    GROUP BY subcorr.cal_id,
             subcorr.date 
    ORDER BY subcorr.cal_id,
             subcorr.date
    ),
        heatmap as (
    SELECT tseries.x, 
           allcalibrados.y, 
           coalesce(countreg.count, 0) count
    FROM tseries
    JOIN allcalibrados ON (allcalibrados.id is not null)
    LEFT JOIN countreg ON (tseries.t=countreg.date AND allcalibrados.id=countreg.id) 
    ),
    datearr as (
		SELECT array_agg(tseries.t::date) dates
		FROM tseries),
	starr as (
		SELECT CASE WHEN _use_id = TRUE
		            THEN array_agg(lpad(allcalibrados.id::text,4,'0') || ' - ' || substring(allcalibrados.nombre,0,12)) 
		            ELSE array_agg(substring(allcalibrados.nombre,0,12))
		       END calibrados
		FROM allcalibrados
	), heatmaparr as (
       SELECT array_agg(ARRAY[heatmap.x::int-1, heatmap.y::int-1, heatmap.count::int]) heatmap
       FROM heatmap)
    SELECT json_build_object('dates',dates,'calibrados',calibrados,'heatmap',heatmap)
    FROM datearr,
         starr,
         heatmaparr;
END;
$function$
;