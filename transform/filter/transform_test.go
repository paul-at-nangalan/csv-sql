package filter

import (
	"csv-sql/transform/shared"
	"testing"
)

func Test_Filter(t *testing.T){
	header := []interface{}{"time", "currency", "asset", "price", "type"}

	cfg := shared.FilterCfg{
		Filter: map[string]bool{
			"time":true,
			"asset":true,
			"price": true,
			"type": false,
		},
		DefaultFilterOut: true,
	}
	exp := []interface{}{"time", "asset", "price"}
	filter := NewFilterTransform(cfg)
	data := []interface{}{"2022-12-01", "USD", "AKK", float64(1.4355), "AAA"}
	expect := []interface{}{"2022-12-01", "AKK", float64(1.4355)}

	overcfg := shared.TransformerCfg{
		Fields: make([]string, 0),
	}
	for _, field := range header{
		overcfg.Fields = append(overcfg.Fields, field.(string))
	}
	filter.Setup(&overcfg)
	header, err := filter.Do(header)
	if err != nil{
		t.Error("Unexpected error ", err)
	}
	for i, exphdr := range exp{
		if header[i].(string) != exphdr{
			t.Error("Mismatch on header ", exphdr, " got ", header[i])
		}
	}
	data, err = filter.Do(data)
	if err != nil{
		t.Error("Unexpected error ", err)
	}
	for i, expdata := range expect {
		switch expdata.(type) {
		case string:
			if expdata.(string) != data[i].(string){
				t.Error("Mismatch data ", expdata, " got ", data[i])
			}
		case float64:
			if expdata.(float64) != data[i].(float64){
				t.Error("Mismatch data ", expdata, " got ", data[i])
			}
		}
	}

}
