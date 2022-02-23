package datetime

import (
	"csv-sql/transform/shared"
	"fmt"
	"testing"
)

func Test_DatetimeTransform(t *testing.T){
	fields := []string{
		"fruit", "price", "date", "colour",
	}
	vals := []interface{}{
		"orange", 2.34, "18/9/2020", "orange",
	}
	cfg := shared.Config{
		Mapping: []shared.DatetimeCfg{
			{Fieldname: "date", From: "2/1/2006", To: "2006-01-02"},
		},
	}
	datetimemap := NewDatetimeTransform(cfg)
	txcfg := shared.TransformerCfg{
		Fields: fields,
	}
	datetimemap.Setup(&txcfg)

	vals, err := datetimemap.Do(vals)
	fmt.Println("Vals are", vals)
	if err != nil{
		t.Error("Unexpected error: ", err)
	}
	if vals[2] != "2020-09-18"{
		t.Error("Mismatch date, expected 2020-09-18 but got ", vals[2])
	}
}
