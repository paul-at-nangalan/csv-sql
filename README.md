# csv-sql
Basic app to convert simple csv to sql inserts

## Example 

### Example file
```
ME Date,ME Ticker,ME Ccy,ME Price
15-Mar-22,HYLD,USD,88.6450
15-Mar-22,IDAP,USD,22.8725
15-Mar-22,AUDHUF,HUF,243.5748

```

### Example table
```
time,ops_code,price,type,source
2022-04-21 00:00:00.000000,DBA,22.61,close,xxx
2022-04-21 00:00:00.000000,SCHF,35.74,close,xxx
2022-04-21 00:00:00.000000,SCHD,79.55,close,xxx

```

### Example transform config

Must be in the config directory (default is ./cfg) and named transforms.json
```
{
		"Headers": {
			"AddCols": {
				"Values": ["type", "source"]
			},
			"RenameCols": {
				"ME Date": "time",
				"ME Ticker": "ops_code",
				"ME Price": "price",
				"ME Ccy": "currency" 
			}
		},
		"Data": {
			"AddData": {
				"Values": ["close", "ib"]
			},
			"CastData": {"price": "float"},
			"FunctionData": {
				"FieldToRule": [
					{
						"ComparisonType": "string",
						"RuleType": "where",
						"Clause": "currency = 'GBp'",
						"UpdateField": "price",
						"UpdateType": "float",
						"UpdateFormula": "price * 0.01"
					}
				]
			},
			"DatetimeData": {
				"Mapping": [
					{"From": "02-Jan-06", "To": "2006-01-02", "Fieldname": "time"}
				]
			},
			"Filter": {
				"DefaultFilterIn": true,
				"Filter": {
					"currency": false
				}
			},
			"TrimSpace":{
				"Fields":{
					"ops_code": true
				}
			}
		}
	}


```

### Example DB config

Must be in the config directory (default is ./cfg) and named postgres.json
```
{
  "Username": "$USER",
  "Password": "$PASSWD",
  "Host":"172.1.7.5",
  "CAFile":"$HOME/rds-ca-2019-root.pem",
  "Database": "my_database",
  "Sslmode": "verify-ca"

}

```
### Example run command

csv-sql --cfg ./cfg/prices/ --db postgres --infile raw-price-data-file.csv --table prices


