package trimspace

import (
	"csv-sql/transform/shared"
	"testing"
)

func TestTrimSpace_Do(t *testing.T) {
	trimspacecfg := shared.TrimSpaceCfg{
		Fields: map[string]bool{
			"aaa": true,
		},
	}
	maincfg := shared.TransformerCfg{
		Fields: []string{"bbb", "aaa", "ccc", "eee"},
	}
	trimsapcetx := NewTrimspaceTransform(trimspacecfg)
	trimsapcetx.Setup(&maincfg)

	vals := []interface{}{1.32, " string with space ", "  should be untrimmed ", "other"}
	expect := []interface{}{1.32, "string with space", "  should be untrimmed ", "other"}

	result, err := trimsapcetx.Do(vals)
	if err != nil{
		t.Error("Unexpected error: ", err)
	}
	for i, expval := range expect{
		switch expval.(type) {
		case string:
			if expval.(string) != result[i].(string){
				t.Error("Mismatch value ", expval, ":", result[i], "@", i)
			}
		case float64:
			if expval.(float64) != result[i].(float64){
				t.Error("Mismatch value ", expval, ":", result[i], "@", i)
			}
		default:
			t.Error("unexpected type ", expval, "@", i)
		}
	}
}
