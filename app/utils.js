
const internal = {}


// internal.control_filter2 = function (valid_filters, filter, default_table) {
// 	// valid_filters = { column1: { table: "table_name", type: "data_type", required: bool}, ... }  
// 	// filter = { column1: "value1", column2: "value2", ....}
// 	// default_table = "table"
// 	var filter_string = " "
// 	var control_flag = 0
// 	Object.keys(valid_filters).forEach(key=>{
// 		var fullkey = (valid_filters[key].table) ? "\"" + valid_filters[key].table + "\".\"" + key + "\"" : (default_table) ? "\"" + default_table + "\".\"" + key + "\"" : "\"" + key + "\""
// 		if(typeof filter[key] != "undefined" && filter[key] !== null && filter[key] != "") {
// 			if(/[';]/.test(filter[key])) {
// 				console.error("Invalid filter value")
// 				control_flag++
// 			}
// 			if(valid_filters[key].type == "regex_string") {
// 				var regex = filter[key].replace('\\','\\\\')
// 				filter_string += " AND " + fullkey  + " ~* '" + filter[key] + "'"
// 			} else if(valid_filters[key].type == "string") {
// 				filter_string += " AND " + fullkey + "='" + filter[key] + "'"
// 			} else if (valid_filters[key].type == "boolean") {
// 				var boolean = (/^[yYtTvVsS1]/.test(filter[key])) ? "true" : "false"
// 				filter_string += " AND "+ fullkey + "=" + boolean + ""
// 			} else if (valid_filters[key].type == "boolean_only_true") {
// 				if (/^[yYtTvVsS1]/.test(filter[key])) {
// 					filter_string += " AND "+ fullkey + "=true"
// 				} 
// 			} else if (valid_filters[key].type == "boolean_only_false") {
// 				if (!/^[yYtTvVsS1]/.test(filter[key])) {
// 					filter_string += " AND "+ fullkey + "=false"
// 				} 
// 			} else if (valid_filters[key].type == "geometry") {
// 				if(! filter[key] instanceof internal.geometry) {
// 					console.error("Invalid geometry object")
// 					control_flag++
// 				} else {
// 					filter_string += "  AND ST_Distance(st_transform(" + fullkey + ",4326),st_transform(" + filter[key].toSQL() + ",4326)) < 0.001" 
// 				}
// 			} else if (valid_filters[key].type == "timestart") {
// 				var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1
// 				if(filter[key] instanceof Date) {
// 					var ldate = new Date(filter[key].getTime()  + offset).toISOString()
// 					filter_string += " AND " + fullkey + ">='" + ldate + "'"
// 				} else {
// 					var d = new Date(filter[key])
// 					var ldate = new Date(d.getTime()  + offset).toISOString()
// 					filter_string += " AND " + fullkey + ">='" + ldate + "'"
// 				}
// 			} else if (valid_filters[key].type == "timeend") {
// 				var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1
// 				if(filter[key] instanceof Date) {
// 					var ldate = new Date(filter[key].getTime()  + offset).toISOString()
// 					filter_string += " AND " + fullkey + "<='" + ldate + "'"
// 				} else {
// 					var d = new Date(filter[key])
// 					var ldate = new Date(d.getTime()  + offset).toISOString()
// 					filter_string += " AND " + fullkey + "<='" + ldate + "'"
// 				}
// 			} else if (valid_filters[key].type == "numeric_interval") {
// 				if(Array.isArray(filter[key])) {
// 					if(filter[key].length < 2) {
// 						console.error("numeric_interval debe ser de al menos 2 valores")
// 						control_flag++
// 					} else {
// 						filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key][0]) + " AND " + key + "<=" + parseFloat(filter[key][1])
// 					}
// 				} else {
// 					 filter_string += " AND " + fullkey + "=" + parseFloat(filter[key])
// 				}
// 			} else {
// 				if(Array.isArray(filter[key])) {
// 					filter_string += " AND "+ fullkey + " IN (" + filter[key].join(",") + ")"
// 				} else {
// 					filter_string += " AND "+ fullkey + "=" + filter[key] + ""
// 				}
// 			}
// 		} else if (valid_filters[key].required) {
// 			console.error("Falta valor para filtro obligatorio " + key)
// 			control_flag++
// 		}
// 	})
// 	if(control_flag > 0) {
// 		return null
// 	} else {
// 		return filter_string
// 	}
// }


internal.not_null = class extends Object {
}

internal.assertValidDateTruncField = function(field) {
	if ([
		"microseconds",
		"milliseconds",
		"second",
		"minute",
		"hour",
		"day",
		"week",
		"month",
		"quarter",
		"year",
		"decade",
		"century",
		"millennium"
	].indexOf(field) < 0) {
		throw(new Error("Invalid date_trunc field: " + field))
	}
}

internal.control_filter2 = function (valid_filters, filter, default_table, crud,throw_on_error=false) {
	// valid_filters = { column1: { table: "table_name", type: "data_type", required: bool, column: "column_name"}, ... }  
	// filter = { column1: "value1", column2: "value2", ....}
	// default_table = "table"
	var filter_string = " "
	var errors  = []
	Object.keys(valid_filters).forEach(key=>{
		var table_prefix = (valid_filters[key].table) ? '"' + valid_filters[key].table + '".' :  (default_table) ? '"' + default_table + '".' : ""
		var column_name = (valid_filters[key].column) ? '"' + valid_filters[key].column + '"' : '"' + key + '"'
		var fullkey = table_prefix + column_name
		if(filter[key] instanceof internal.not_null) {
			filter_string += ` AND ` + fullkey + ` IS NOT NULL `
		} else if(typeof filter[key] != "undefined" && filter[key] !== null) {
			if(/[';]/.test(filter[key])) {
				errors.push("Invalid filter value")
				console.error(errors[errors.length-1])
			}
			if(valid_filters[key].type == "regex_string") {
				var regex = filter[key].replace('\\','\\\\')
				filter_string += " AND " + fullkey  + " ~* '" + filter[key] + "'"
			} else if(valid_filters[key].type == "string") {
				if(Array.isArray(filter[key])) {
					var values = filter[key].filter(v=>v != null).map(v=>v.toString()).filter(v=>v != "")
                    if(!values.length) {
						errors.push("Empty or invalid string array")
						console.error(errors[errors.length-1])
                    } else {
						if(valid_filters[key].case_insensitive) {
							filter_string += ` AND lower(${fullkey}) IN ( ${values.map(v=>`lower('${v}')`).join(",")})`
						} else {
							filter_string += ` AND ${fullkey} IN ( ${values.map(v=>`'${v}'`).join(",")})`
						}
                    }
				} else {
					if(valid_filters[key].case_insensitive) {
						filter_string += ` AND lower(${fullkey})=lower('${filter[key]}')`
					} else {
						filter_string += " AND "+ fullkey + "='" + filter[key] + "'"
					}
				}
			} else if (valid_filters[key].type == "boolean") {
				var boolean = (/^[yYtTvVsS1]/.test(filter[key])) ? "true" : "false"
				filter_string += " AND "+ fullkey + "=" + boolean + ""
			} else if (valid_filters[key].type == "boolean_only_true") {
				if (/^[yYtTvVsS1]/.test(filter[key])) {
					filter_string += " AND "+ fullkey + "=true"
				} 
			} else if (valid_filters[key].type == "boolean_only_false") {
				if (!/^[yYtTvVsS1]/.test(filter[key])) {
					filter_string += " AND "+ fullkey + "=false"
				} 
			} else if (valid_filters[key].type == "geometry") {
				if(! filter[key] instanceof crud.geometry) {
					errors.push("Invalid geometry object")
					console.error(errors[errors.length-1])
				} else {
					filter_string += "  AND ST_Distance(st_transform(" + fullkey + ",4326),st_transform(" + filter[key].toSQL() + ",4326)) < 0.001" 
				}
			} else if (valid_filters[key].type == "date" || valid_filters[key].type == "timestamp") {
                let d
				if(filter[key] instanceof Date) {
                    d = filter[key]
                } else {
                    d = new Date(filter[key])
                }
				if(valid_filters[key].trunc != undefined) {
					internal.assertValidDateTruncField(valid_filters[key].trunc)
					filter_string += ` AND date_trunc('${valid_filters[key].trunc}',${fullkey}) = date_trunc('${valid_filters[key].trunc}', '${d.toISOString()}'::timestamptz)`	
				} else {
					filter_string += " AND " + fullkey + "='" + d.toISOString() + "'::timestamptz"
				}
            } else if (valid_filters[key].type == "timestart") {
				var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1
				if(filter[key] instanceof Date) {
					var ldate = new Date(filter[key].getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + ">='" + ldate + "'"
				} else {
					var d = new Date(filter[key])
					var ldate = new Date(d.getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + ">='" + ldate + "'"
				}
			} else if (valid_filters[key].type == "timeend") {
				var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1
				if(filter[key] instanceof Date) {
					var ldate = new Date(filter[key].getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + "<='" + ldate + "'"
				} else {
					var d = new Date(filter[key])
					var ldate = new Date(d.getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + "<='" + ldate + "'"
				}
			} else if (valid_filters[key].type == "greater_or_equal_date") {
				var ldate = new Date(filter[key]).toISOString()
				filter_string += ` AND ${fullkey} >= '${ldate}'::timestamptz`
			} else if (valid_filters[key].type == "smaller_or_equal_date") {
				var ldate = new Date(filter[key]).toISOString()
				filter_string += ` AND ${fullkey} <= '${ldate}'::timestamptz`
			} else if (valid_filters[key].type == "numeric_interval") {
				if(Array.isArray(filter[key])) {
					if(filter[key].length < 2) {
						errors.push("numeric_interval debe ser de al menos 2 valores")
						console.error(errors[errors.length-1])
					} else {
						filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key][0]) + " AND " + key + "<=" + parseFloat(filter[key][1])
					}
				} else {
					 filter_string += " AND " + fullkey + "=" + parseFloat(filter[key])
				}
			} else if(valid_filters[key].type == "numeric_min") {
				filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key])
			} else if(valid_filters[key].type == "numeric_max") {
				filter_string += " AND " + fullkey + "<=" + parseFloat(filter[key])
            } else if (valid_filters[key].type == "integer") {
                if(Array.isArray(filter[key])) {
                    var values = filter[key].map(v=>parseInt(v)).filter(v=>v.toString()!="NaN")
                    if(!values.length) {
						errors.push(`Invalid integer array : ${filter[key].toString()}`)
						console.error(errors[errors.length-1])
                    } else {
    					filter_string += " AND "+ fullkey + " IN (" + values.join(",") + ")"
                    }
				} else {
                    var value = parseInt(filter[key])
                    if(value.toString() == "NaN") {
						errors.push(`Invalid integer: ${filter[key]}`)
						console.error(errors[errors.length-1])
                    } else {
                        filter_string += " AND "+ fullkey + "=" + value + ""
                    }
				}
            } else if (valid_filters[key].type == "number" || valid_filters[key].type == "float") {
                if(Array.isArray(filter[key])) {
                    var values = filter[key].map(v=>parseFloat(v)).filter(v=>v.toString()!="NaN")
                    if(!values.length) {
						errors.push(`Invalid float array: ${filter[key].toString()}`)
						console.error(errors[errors.length-1])
                    } else {
                        filter_string += " AND " + fullkey + " IN (" + values.join(",") + ")"
                    }
                } else {
                    var value = parseFloat(filter[key])
                    if(value.toString() == "NaN") {
						errors.push(`Invalid float: ${filter[key]}`)
						console.error(errors[errors.length-1])
                    } else {
                        filter_string += " AND " + fullkey + "=" + value + ""
                    }
                }
			} else if (valid_filters[key].type == "interval") {
				var value = timeSteps.createInterval(filter[key])
				if(!value) {
					throw("invalid interval filter: " + filter[key])
				}
				filter_string += ` AND ${fullkey}='${value.toPostgres()}'::interval` 
			} else {
				if(Array.isArray(filter[key])) {
					filter_string += " AND "+ fullkey + " IN (" + filter[key].join(",") + ")"
				} else {
					filter_string += " AND "+ fullkey + "=" + filter[key] + ""
				}
			}
		} else if (valid_filters[key].required) {
			errors.push("Falta valor para filtro obligatorio " + key)
			console.error(errors[errors.length-1])
		}
	})
	if(errors.length > 0) {
		if(throw_on_error) {
			throw("Invalid filter:\n" + errors.join("\n"))
		} else {
			return null
		}
	} else {
		return filter_string
	}
}

internal.pasteIntoSQLQuery = function(query,params) {
	for(var i=params.length-1;i>=0;i--) {
		var value
		switch(typeof params[i]) {
			case "string":
				value = "'" + params[i] + "'"
				break;
			case "number":
				value = params[i]
				break
			case "object":
				if(params[i] instanceof Date) {
					value = "'" + params[i].toISOString() + "'::timestamptz::timestamp"
				} else if(params[i] instanceof Array) {
					value = "{" + params[i].map(v=> (typeof v == "number") ? v : "'" + v.toString() + "'").join(",") + "}"
				} else {
					if(params[i] === null) {
						value = "NULL"
					} else {
						value = params[i].toString()
					}
				}
				break;
			case "undefined": 
				value = "NULL"
				break;
			default:
				value = "'" + params[i].toString() + "'"
		}
		var I = parseInt(i)+1
		var placeholder = "\\$" + I.toString()
		// console.log({placeholder:placeholder,value:value})
		query = query.replace(new RegExp(placeholder,"g"), value)
	}
	return query
}

module.exports = internal