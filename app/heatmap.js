'use strict'

const {control_filter2, pasteIntoSQLQuery} = require('./utils')

const internal = {}

// returns 1d observation density vector
internal.heatmap = class {
    constructor(pool)  {
        this.pool=pool
    }
    obsDensityVector (timestart,timeend,dt="1 day",filter={},options={}) {
        if(!timestart) {
            timestart = new Date()
            timestart = new Date(timestart.getFullYear(),timestart.getMonth(),timestart.getDay())
            timestart.setMonth(timestart.getMonth() - 1)
        }
        if(!timeend) {
            timeend = new Date()
        }
        if(new Date(timestart).toString() == "Invalid Date" || new Date(timeend).toString() == "Invalid Date" || !dt) {
            return Promise.reject("Missing or bad parameters: timestart,timeend,dt")
        }
        var valid_filters= {"var_id": {table:"series",type:"integer"},"tabla_id":{table:"redes",type:"string"},"unit_id":{table:"series",type:"integer"},"estacion_id":{table:"series",type:"integer"},"proc_id":{table:"series",type:"integer"}}
        var count_filters = 0
        Object.keys(valid_filters).forEach(key=>{
            if(filter[key]) {
                count_filters++
            }
        })
        // console.log({count_filters:count_filters})
        if(count_filters == 0) {
            return Promise.reject("Missing at least one filter: var_id, proc_id, unit_id, tabla_id, estacion_id")
        }
        var filter_string = control_filter2(valid_filters,filter)
        var query  = "WITH ts as (\
            SELECT generate_series($1::timestamp,$2::timestamp,$3::interval) t\
            ),\
            tseries as (\
            SELECT t,\
                   row_number() OVER (order by t) x\
            FROM ts\
            ),\
			obs AS (\
			SELECT observaciones.timestart\
			FROM observaciones,series,estaciones,redes\
            WHERE observaciones.timestart >= $1::timestamp\
            AND observaciones.timestart < $2::timestamp\
            AND observaciones.series_id=series.id\
            AND series.estacion_id=estaciones.unid\
            AND estaciones.tabla=redes.tabla_id\
            " + filter_string + ")\
            SELECT tseries.x,\
				   tseries.t,\
                   count(obs.timestart)\
            FROM tseries\
			LEFT JOIN obs ON (obs.timestart >= tseries.t\
            AND obs.timestart < tseries.t+$3::interval)\
            GROUP BY x,t\
            ORDER BY x;"
		var query_string = pasteIntoSQLQuery(query,[timestart,timeend,dt])
        console.log(query_string)
        return this.pool.query(query_string) // query,[timestart,timeend,dt])
        .then(result=>{
            if (!result.rows) {
                throw("Query error")
            }
            var values = result.rows.map(row=>row.count)
			if(options.format && options.format == "tuples") {
				return result.rows.map(row=>{
					return [row.x, row.t, row.count]
				})
			}
            if(options.include_timestamps) {
                return {
                    values: values,
                    timestamps: result.rows.map(row=>row.t)
                }
            } else {
                return {
                    values: values
                }
            }
        })
    }
	stationDensityVector (timestart,timeend,dt="1 day",filter={},options={}) {
        if(!timestart) {
            timestart = new Date()
            timestart = new Date(timestart.getFullYear(),timestart.getMonth(),timestart.getDay())
            timestart.setMonth(timestart.getMonth() - 1)
        }
        if(!timeend) {
            timeend = new Date()
        }
        if(new Date(timestart).toString() == "Invalid Date" || new Date(timeend).toString() == "Invalid Date" || !dt) {
            return Promise.reject("Missing or bad parameters: timestart,timeend,dt")
        }
        var valid_filters= {"var_id": {table:"series",type:"integer"},"tabla_id":{table:"redes",type:"string"},"unit_id":{table:"series",type:"integer"},"estacion_id":{table:"series",type:"integer"},"proc_id":{table:"series",type:"integer"}}
        var count_filters = 0
        Object.keys(valid_filters).forEach(key=>{
            if(filter[key]) {
                count_filters++
            }
        })
        // console.log({count_filters:count_filters})
        if(count_filters == 0) {
            return Promise.reject("Missing at least one filter: var_id, proc_id, unit_id, tabla_id, estacion_id")
        }
        var filter_string = control_filter2(valid_filters,filter)
        var query  = "WITH ts as (\
            SELECT generate_series($1::timestamp,$2::timestamp,$3::interval) t\
            ),\
            tseries as (\
            SELECT t,\
                   row_number() OVER (order by t) x\
            FROM ts\
            ),\
			obs AS (\
			SELECT observaciones.timestart,\
			       series.estacion_id\
			FROM observaciones,series,estaciones,redes\
            WHERE observaciones.timestart >= $1::timestamp\
            AND observaciones.timestart < $2::timestamp\
            AND observaciones.series_id=series.id\
            AND series.estacion_id=estaciones.unid\
            AND estaciones.tabla=redes.tabla_id\
            " + filter_string + ")\
            SELECT tseries.x,\
				   tseries.t,\
                   count(DISTINCT obs.estacion_id)\
            FROM tseries\
			LEFT JOIN obs ON (obs.timestart >= tseries.t\
            AND obs.timestart < tseries.t+$3::interval)\
            GROUP BY x,t\
            ORDER BY x;"
		var query_string = pasteIntoSQLQuery(query,[timestart,timeend,dt])
        // console.log(query_string)
        return this.pool.query(query_string) // query,[timestart,timeend,dt])
        .then(result=>{
            if (!result.rows) {
                throw("Query error")
            }
            var values = result.rows.map(row=>row.count)
			if(options.format && options.format == "tuples") {
				return result.rows.map(row=>{
					return [row.x, row.t, row.count]
				})
			}
            if(options.include_timestamps) {
                return {
                    values: values,
                    timestamps: result.rows.map(row=>row.t)
                }
            } else {
                return {
                    values: values
                }
            }
        })
    }

    /**
     * @typedef DensityVector
     * @property {integer[]} values
     * @property {Date[]} [timestamps]
     */
	
    /**
     * 
     * @param {Date} timestart 
     * @param {Date} timeend 
     * @param {string} [dt] - time step
     * @param {Object} [filter]
     * @param {integer} filter.var_id
     * @param {integer} filter.unit_id
     * @param {integer} filter.proc_id
     * @param {integer} filter.cal_id
     * @param {integer} filter.cal_grupo_id
     * @param {integer} filter.fuentes_id
     * @param {integer} filter.area_id
     * @param {integer} filter.escena_id
     * @param {integer} filter.tabla_id
     * @param {integer} filter.estacion_id
     * @param {Date} filter.forecast_date
     * @param {('pronosticos','corridas')} [filter.count] - default pronosticos
     * @param {Object} [options]
     * @param {string} options.format 
     * @param {boolean} options.include_timestamps
     * @returns {DensityVector} densityVector
     */
    async pronoDensityVector (timestart,timeend,dt="1 day",filter={},options={}) {
		if(!timestart) {
			timestart = new Date()
			timestart = new Date(timestart.getFullYear(),timestart.getMonth(),timestart.getDay())
			timestart.setMonth(timestart.getMonth() - 1)
		}
		if(!timeend) {
			timeend = new Date()
			timeend.setDate(timeend.getDate()+14)
		}
		if(new Date(timestart).toString() == "Invalid Date" || new Date(timeend).toString() == "Invalid Date" || !dt) {
			return Promise.reject("Missing or bad parameters: timestart,timeend,dt")
		}
		var valid_filters= {
            "var_id": {type:"integer"},
            "unit_id":{type:"integer"},
            "proc_id":{type:"integer"},
            "series_id":{type:"integer", column: "id"},
        }
        if(filter.count == "corridas") {
            valid_filters.cal_id = {table: "corridas",type: "integer"}
            valid_filters.cal_grupo_id = {table: "calibrados",type:"integer", column: "grupo_id"}
            valid_filters.cor_id = {table: "corridas",type: "integer", column: "id"}
            valid_filters.forecast_date = {table: "corridas", type: "date", column: "date"}
        } else {            
            valid_filters.cal_id = {table:"series_prono_date_range_last",type:"integer"},
            valid_filters.cal_grupo_id = {table:"series_prono_date_range_last",type:"integer"}
            valid_filters.cor_id = {table: "series_prono_date_range_last",type: "integer"}
            valid_filters.forecast_date = {table: "series_prono_date_range_last", type: "date"}
        }
        if(filter.tipo == "areal") {
            var prono_table =  "pronosticos_areal"
            valid_filters.fuentes_id = {type:"integer"}
            valid_filters.area_id = {type:"integer"}
            var series_table = "series_areal"
            var extra_joins = "" // ["areas_pluvio","fuentes"]
        } else if (filter.tipo == "raster" || filter.tipo == "rast") {
            var prono_table = "pronosticos_rast"
            valid_filters.fuentes_id = {table:"series_areal",type:"integer"}
            valid_filters.escena_id = {type:"integer"}
            var series_table = "series_rast"
            var extra_joins = ""
        } else {
            var prono_table = "pronosticos"
            valid_filters.tabla_id = {table:"redes",type:"string"}
            valid_filters.estacion_id = {type:"integer"}
            var series_table = "series"
            var extra_joins = `
                JOIN estaciones 
                    ON (series.estacion_id = estaciones.unid)
                JOIN redes
                    ON (estaciones.tabla = redes.tabla_id)
            `
        }
		var count_filters = 0
		Object.keys(valid_filters).forEach(key=>{
			if(filter[key]) {
				count_filters++
			}
		})
		// console.log({count_filters:count_filters})
		if(count_filters == 0) {
			return Promise.reject("Missing at least one filter: " + Object.keys(valid_filters).join(", "))
		}

        // console.debug({filter:filter})
        
        try {
    		var filter_string = control_filter2(valid_filters,filter, series_table, undefined, true)
        } catch(e) {
            throw(new Error(e))
        }
        var prono_query = (filter.count == "corridas") ? `
                SELECT 
                    corridas.date AS timestart
                FROM corridas
                JOIN calibrados
                    ON (corridas.cal_id = calibrados.id)
                JOIN series_prono_date_range
                    ON (corridas.id = series_prono_date_range.cor_id)
                JOIN ${series_table} 
                    ON (series_prono_date_range.series_id = ${series_table}.id)
                ${extra_joins}
                WHERE corridas.date >= $1::timestamp
                AND corridas.date < $2::timestamp  + $3::interval
                ${filter_string}
                AND series_prono_date_range.series_table = '${series_table}'
            ` : `
                SELECT 
                    ${prono_table}.timestart
                FROM ${prono_table}
                JOIN series_prono_date_range_last
                    ON (${prono_table}.cor_id = series_prono_date_range_last.cor_id)
                JOIN ${series_table} 
                    ON (${prono_table}.series_id = ${series_table}.id)
                ${extra_joins}
                WHERE ${prono_table}.timestart >= $1::timestamp
                AND ${prono_table}.timestart < $2::timestamp + $3::interval
                ${filter_string}
                AND series_prono_date_range_last.series_table = '${series_table}'
            `
		var query  = `WITH ts as (
			SELECT generate_series($1::timestamp,$2::timestamp,$3::interval) t
			),
			tseries as (
			SELECT t,
				   row_number() OVER (order by t) x
			FROM ts
			),
			prono AS (${prono_query})
			SELECT tseries.x,
				   tseries.t,
				   count(prono.timestart)
			FROM tseries
			LEFT JOIN prono ON (prono.timestart >= tseries.t
			AND prono.timestart < tseries.t+$3::interval)
			GROUP BY x,t
			ORDER BY x`
		var query_string = pasteIntoSQLQuery(query,[timestart,timeend,dt])
		// console.debug(query_string)
        try {
    		var result = await this.pool.query(query_string) // query,[timestart,timeend,dt])
        } catch(e) {
            throw(new Error(e))
        }
        if (!result.rows) {
            throw(new Error("Query error"))
        }
        var values = result.rows.map(row=>row.count)
        if(options.format && options.format == "tuples") {
            return result.rows.map(row=>{
                return [row.x, row.t, row.count]
            })
        }
        if(options.include_timestamps) {
            return {
                values: values,
                timestamps: result.rows.map(row=>row.t)
            }
        } else {
            return {
                values: values
            }
        }
	}
	obsArealDensityVector (timestart,timeend,dt="1 day",filter={},options={}) {
        if(!timestart) {
            timestart = new Date()
            timestart = new Date(timestart.getFullYear(),timestart.getMonth(),timestart.getDay())
            timestart.setMonth(timestart.getMonth() - 1)
        }
        if(!timeend) {
            timeend = new Date()
        }
        if(new Date(timestart).toString() == "Invalid Date" || new Date(timeend).toString() == "Invalid Date" || !dt) {
            return Promise.reject("Missing or bad parameters: timestart,timeend,dt")
        }
        var valid_filters= {"var_id": {table:"series_areal",type:"integer"},"fuentes_id":{table:"series_areal",type:"integer"},"unit_id":{table:"series_areal",type:"integer"},"area_id":{table:"series_areal",type:"integer"},"proc_id":{table:"series_areal",type:"integer"}}
        var count_filters = 0
        Object.keys(valid_filters).forEach(key=>{
            if(filter[key]) {
                count_filters++
            }
        })
        // console.log({count_filters:count_filters})
        if(count_filters == 0) {
            return Promise.reject("Missing at least one filter: var_id, proc_id, unit_id, fuentes_id, area_id")
        }
        var filter_string = control_filter2(valid_filters,filter)
        var query  = "WITH ts as (\
            SELECT generate_series($1::timestamp,$2::timestamp,$3::interval) t\
            ),\
            tseries as (\
            SELECT t,\
                   row_number() OVER (order by t) x\
            FROM ts\
            ),\
			obs AS (\
			SELECT observaciones_areal.timestart\
			FROM observaciones_areal,series_areal\
            WHERE observaciones_areal.timestart >= $1::timestamp\
            AND observaciones_areal.timestart < $2::timestamp\
            AND observaciones_areal.series_id=series_areal.id\
            " + filter_string + ")\
            SELECT tseries.x,\
				   tseries.t,\
                   count(obs.timestart)\
            FROM tseries\
			LEFT JOIN obs ON (obs.timestart >= tseries.t\
            AND obs.timestart < tseries.t+$3::interval)\
            GROUP BY x,t\
            ORDER BY x;"
		var query_string = pasteIntoSQLQuery(query,[timestart,timeend,dt])
        console.log(query_string)
        return this.pool.query(query_string) // query,[timestart,timeend,dt])
        .then(result=>{
            if (!result.rows) {
                throw("Query error")
            }
            var values = result.rows.map(row=>row.count)
			if(options.format && options.format == "tuples") {
				return result.rows.map(row=>{
					return [row.x, row.t, row.count]
				})
			}
            if(options.include_timestamps) {
                return {
                    values: values,
                    timestamps: result.rows.map(row=>row.t)
                }
            } else {
                return {
                    values: values
                }
            }
        })
    }
	obsRastDensityVector(timestart,timeend,dt="1 day",filter={},options={}) {
        if(!timestart) {
            timestart = new Date()
            timestart = new Date(timestart.getFullYear(),timestart.getMonth(),timestart.getDay())
            timestart.setMonth(timestart.getMonth() - 1)
        }
        if(!timeend) {
            timeend = new Date()
        }
        if(new Date(timestart).toString() == "Invalid Date" || new Date(timeend).toString() == "Invalid Date" || !dt) {
            return Promise.reject("Missing or bad parameters: timestart,timeend,dt")
        }
        var valid_filters= {"var_id": {table:"series_rast",type:"integer"},"fuentes_id":{table:"series_rast",type:"integer"},"unit_id":{table:"series_rast",type:"integer"},"escena_id":{table:"series_rast",type:"integer"},"proc_id":{table:"series_rast",type:"integer"},"series_id":{table:"observaciones_rast",type:"integer"}}
        var count_filters = 0
        Object.keys(valid_filters).forEach(key=>{
            if(filter[key]) {
                count_filters++
            }
        })
        // console.log({count_filters:count_filters})
        if(count_filters == 0) {
            return Promise.reject("Missing at least one filter: var_id, proc_id, unit_id, fuentes_id, escena_id")
        }
        var filter_string = control_filter2(valid_filters,filter)
        var query  = "WITH ts as (\
            SELECT generate_series($1::timestamp,$2::timestamp,$3::interval) t\
            ),\
            tseries as (\
            SELECT t,\
                   row_number() OVER (order by t) x\
            FROM ts\
            ),\
			obs AS (\
			SELECT observaciones_rast.timestart\
			FROM observaciones_rast,series_rast\
            WHERE observaciones_rast.timestart >= $1::timestamp\
            AND observaciones_rast.timestart < $2::timestamp\
            AND observaciones_rast.series_id=series_rast.id\
            " + filter_string + ")\
            SELECT tseries.x,\
				   tseries.t,\
                   count(obs.timestart)\
            FROM tseries\
			LEFT JOIN obs ON (obs.timestart >= tseries.t\
            AND obs.timestart < tseries.t+$3::interval)\
            GROUP BY x,t\
            ORDER BY x;"
		var query_string = pasteIntoSQLQuery(query,[timestart,timeend,dt])
        console.log(query_string)
        return this.pool.query(query_string) // query,[timestart,timeend,dt])
        .then(result=>{
            if (!result.rows) {
                throw("Query error")
            }
            var values = result.rows.map(row=>row.count)
			if(options.format && options.format == "tuples") {
				return result.rows.map(row=>{
					return [row.x, row.t, row.count]
				})
			}
            if(options.include_timestamps) {
                return {
                    values: values,
                    timestamps: result.rows.map(row=>row.t)
                }
            } else {
                return {
                    values: values
                }
            }
        })
    }
}

// function getStationDensityVector() {}


module.exports = internal
