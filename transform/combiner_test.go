package transform

import (
	"csv-sql/transform/shared"
	"testing"
)

func Test_Combiner(t *testing.T){

	header := []interface{}{"ME date", "Sett date", "ME currency", "ME instrument", "ticker", "ME price", "ME factor"}
	expheader := []string{"time", "settle_date", "currency", "asset", "price", "factor", "type", "source"}
	txconf := shared.TransformerCfg{
		Headers: shared.HeaderTransforms{
			AddCols: shared.CfgAddColumn{
				Values: []interface{}{"type", "source"},
			},
			RenameCols: map[string]string{
				"ME date": "time",
				"ME instrument": "asset",
				"ME price": "price",
				"ME currency": "currency",
				"ME factor": "factor",
				"Sett date": "settle_date",
			},
		},
		Data: shared.DataTransforms{
			AddData: shared.CfgAddColumn{
				Values: []interface{}{"close", "aib"},
			},
			FunctionData: shared.FunctionMapCfg{
				FieldToRule: []shared.Rule{
					{
						ComparisonType: "string",
						RuleType: "where",
						Clause: "currency = 'GBp'",
						UpdateField: "price",
						UpdateType: "float",
						UpdateFormula: "price * 0.01",
					},
					{
						ComparisonType: "float",
						RuleType: "where",
						Clause: "factor > 1",
						UpdateField: "currency",
						UpdateType: "string",
						UpdateFormula: "currency + ' 100'",
					},
				},
			},
			DatetimeData: shared.Config{
				Mapping: []shared.DatetimeCfg{
					{From: "2/1/2006", To: "2006-01-02", Fieldname: "time"},
					{From: "2/1/2006", To: "2006-01-02", Fieldname: "settle_date"},
				},
			},
			Filter: shared.FilterCfg{
				DefaultFilterIn: true,
				Filter: map[string]bool{
					"ticker": false,
				},
			},
		},
	}

	combiner := NewCombinerWithConfig(&txconf)
	header, err := combiner.DoHeader(header)
	if err != nil{
		t.Error("Unexpected error ", err)
	}
	for i, colname := range expheader{
		if header[i].(string) != colname{
			t.Error("Mismatch header name, expected ", colname, " got ", header[i])
		}
	}

	vals := []interface{}{
		"19/2/2022", "19/2/2022", "USD", "BIN", "BIN", 1.233, float64(1)}
	expect := []interface{}{
		"2022-02-19", "2022-02-19","USD", "BIN", float64(1.233), float64(1), "close", "aib"}
	runDataTest(vals, expect, t, combiner)

	vals = []interface{}{
		"19/2/2022",  "19/2/2022","GBp", "AKK", "AKK", 1233, float64(100)}
	expect = []interface{}{
		"2022-02-19", "2022-02-19","GBp 100", "AKK", float64(12.33), float64(100), "close", "aib"}
	runDataTest(vals, expect, t, combiner)

	vals = []interface{}{
		"19/2/2022",  "19/2/2022","GBp", "AKK", "AKK", 1233, float64(1)}
	expect = []interface{}{
		"2022-02-19", "2022-02-19","GBp", "AKK", float64(12.33), float64(1), "close", "aib"}
	runDataTest(vals, expect, t, combiner)

	vals = []interface{}{
		"19/2/2022",  "19/2/2022","GBP", "AKK", "AKK", 12.33, float64(1)}
	expect = []interface{}{
		"2022-02-19", "2022-02-19","GBP", "AKK", float64(12.33), float64(1), "close", "aib"}
	runDataTest(vals, expect, t, combiner)
}

func runDataTest(vals, expect []interface{}, t *testing.T, combiner *Combiner){
	vals, err := combiner.DoData(vals)
	if err != nil{
		t.Error("Unexpected error ", err)
	}
	for i, expval := range expect{
		switch expval.(type) {
		case string:
			if expval != vals[i].(string){
				t.Error("Mismatch value, expected ", expval, " got ", vals[i])
			}
		case float64:
			if expval != vals[i].(float64){
				t.Error("Mismatch value, expected ", expval, " got ", vals[i])
			}
		}
	}
}
