# variables = {
#     "incoming variable name from data provider": {
#         "vargem": "Synoptic Variable Table Vargem",
#         "VNUM": "Synotic VNUM",
#         "long_name": "Long name description",
#         "incoming_unit": "incoming data unit from data provider",
#         "final_unit": "final unit that POE is expecting (should be metric unit if keeping units in json slug)"
#     }
# }

# for example, here is a key from the variables dict used in the nasa globe ingest
variables = {
    "air_temperature": {
        "vargem": "TMPF",
        "VNUM": "1",
        "long_name": "Air Temperature",
        "incoming_unit": "degC",
        "final_unit": "degC"
    },
    "relative_humidity": {
        "vargem": "RELH",
        "VNUM": "1",
        "long_name": "Relative Humidity",
        "incoming_unit": "pct",
        # "%"
        "final_unit": "pct"
    },
    "rainfall": {
        "vargem": "PREC",
        "VNUM": "1",
        "long_name": "Rainfall",
        "incoming_unit": "mm",
        "final_unit": "mm"
    },
    "wind_speed": {
        "vargem": "SKNT",
        "VNUM": "1",
        "long_name": "Wind Speed",
        "incoming_unit": "KT",
        "final_unit": "mps"         
    },    
     "wind_direction": {
        "vargem": "DRCT",
        "VNUM": "1",
        "long_name": "Wind Direction",
        "incoming_unit": "deg",
        "final_unit": "deg"         
    },
},