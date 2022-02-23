package transform

import (
	"csv-sql/transform/addcol"
	"csv-sql/transform/datetime"
	"csv-sql/transform/function"
	"testing"
)

func Test_Combiner(t *testing.T){

	header := []interface{}{"ME date", "Sett date", "ME currency", "ME instrument", "ticker", "ME price", "ME factor"}
	expheader := []string{"time", "currency", "asset", "ticker", "price", "factor", "type", "source"}
	txconf := TransformerCfg{
		Headers: HeaderTransforms{
			AddCols: addcol.CfgAddColumn{
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
		Data: DataTransforms{
			AddData: addcol.CfgAddColumn{
				Values: []interface{}{"close", "aib"},
			},
			FunctionData: function.FunctionMapCfg{
				FieldToRule: []function.Rule{
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
			DatetimeData: datetime.Config{
				Mapping: []datetime.DatetimeCfg{
					{From: "2/1/2006", To: "2022-02-14", Fieldname: "time"},
					{From: "2/1/2006", To: "2022-02-14", Fieldname: "settle_date"},
				},
			},
		},
	}

	combiner := NewCombinerWithConfig(&txconf)
	header, err := combiner.DoHeader(header)

	vals := []interface{}{
		"19/2/2022", "USD", "BIN", "BIN", "1.233", "1"}
	expect := []interface{}{
		"2022-02-19", "USD", "BIN", "BIN", "1.233", "1", "close", "aib"}

}
