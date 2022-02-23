package function

import (
	"csv-sql/transform"
	"fmt"
	"log"
	"regexp"
	"testing"
)

func getStdTestFields()(vals []interface{}, fields []string){
	vals = []interface{}{
		1.2988,
		"GBp",
		"Orange",
		9.927,
		9.928,
		"Green",
	}
	fields = []string{
		"price",
		"currency",
		"fruit",
		"height",
		"width",
		"colour",
	}
	return
}


func newFunctionRemap()*FunctionRemap{
	match := regexp.MustCompile("([a-zA-Z0-9_]*)[\\s]*([=!<>]{1,})[\\s]*(.*)")
	return &FunctionRemap{
		fieldindexes: make(map[string]int),
		matchclause: match,
	}
}

func findIndex(fields []string, name string)int{
	for i, colname := range fields{
		if colname == name{
			return i
		}
	}
	log.Panicln("Failed to find field ", name, " in test setup")
	return 0
}

type TestCase struct{
	ruletype string
	first float64
	second float64

	firststr string
	secondstr string

	expmatch bool
}

func runMatchTestFloat(vals []interface{}, tcase TestCase,
	testclause Rule, firstindx, secondindx int, first, second string,
		remapper *FunctionRemap, t *testing.T){

	vals[firstindx] = tcase.first
	testclause.ComparisonType = CMPTYPE_FLOAT
	if secondindx != -1 {
		//// Compare 2 fields
		vals[secondindx] = tcase.second
		testclause.Clause = first + tcase.ruletype + second
	}else{
		testclause.Clause = first + tcase.ruletype + fmt.Sprintf("%f",tcase.second)
	}
	ismatched, err := remapper.handleWhere(vals, testclause)
	if err != nil {
		t.Error("Unexpected error ", err)
	}
	if ismatched != tcase.expmatch{
		t.Error("Mismatched outcome, expected ", tcase.expmatch, " got ", ismatched,
			"{", tcase, "}")
	}
}

func runMatchTestString(vals []interface{}, tcase TestCase,
	testclause Rule, firstindx, secondindx int, first, second string,
	remapper *FunctionRemap, t *testing.T){

	vals[firstindx] = tcase.firststr
	testclause.ComparisonType = CMPTYPE_STRING
	if secondindx != -1 {
		//// Compare 2 fields
		vals[secondindx] = tcase.secondstr
		testclause.Clause = first + tcase.ruletype + second
	}else{
		testclause.Clause = first + tcase.ruletype + "'" + tcase.secondstr + "'"
	}
	ismatched, err := remapper.handleWhere(vals, testclause)
	if err != nil {
		t.Error("Unexpected error ", err)
	}
	if ismatched != tcase.expmatch{
		t.Error("Mismatched outcome, expected ", tcase.expmatch, " got ", ismatched,
			"{", tcase, "}")
	}
}

func getStdCases(fields []string)(cases []TestCase, first, second string, firstindx, secondindx int){

	cases = []TestCase{
		{CMPTYPE_E, 501.233, 501.233, "Aaa bc", "Aaa bc", true},
		{CMPTYPE_E, 501.233, 501.234, "Bddc", "Ddds", false},
		{CMPTYPE_NE, 455.99912, 455.8, "Sxxcd", "Xdf", true},
		{CMPTYPE_NE, 455.8, 455.8, "sdff", "sdff", false},
		{CMPTYPE_L, 41.2, 42.34, "aaa", "ddd", true},
		{CMPTYPE_L, 401.2, 42.34, "aaa", "aaa", false},
		{CMPTYPE_L, 41.2, 41.2, "bbb", "aaa", false},
		{CMPTYPE_LE, 41.2, 42.34, "aaad", "bbbd", true},
		{CMPTYPE_LE, 41.2, 41.2, "ddee", "ddee", true},
		{CMPTYPE_LE, 41.2, 10.2, "ddde", "aaaa", false},
		{CMPTYPE_G, 432.87, 56.34, "ddde", "aaa", true},
		{CMPTYPE_G, 43.87, 56.34, "aaad", "ddde", false},
		{CMPTYPE_GE, 432.87, 431, "eeee", "aaaa", true},
		{CMPTYPE_GE, 432.87, 432.87, "ddee", "ddee", true},
		{CMPTYPE_GE, 431, 432.87, "aaaa", "dddd", false},
	}
	first = "height"
	second = "width"
	firstindx = findIndex(fields, first)
	secondindx = findIndex(fields, second)
	return
}

func Test_ClauseMatchingFloat(t *testing.T){
	vals, fields := getStdTestFields()
	remapper := newFunctionRemap()
	cfg := transform.TransformerCfg{
		Fields: fields,
	}
	remapper.Setup(&cfg)

	testclause := Rule{ }
	cases, first, second, firstindx, secondindx := getStdCases(fields)

	for _, tcase := range cases {
		fmt.Println("Test match 1")
		runMatchTestFloat(vals, tcase, testclause, firstindx, secondindx,
			first, second, remapper, t)
		fmt.Println("Test match 2")
		runMatchTestFloat(vals, tcase, testclause, firstindx, -1,
			first, second, remapper, t)
		fmt.Println("Test match 3")
		runMatchTestString(vals, tcase, testclause, firstindx, secondindx,
			first, second, remapper, t)
		fmt.Println("Test match 4")
		runMatchTestString(vals, tcase, testclause, firstindx, -1,
			first, second, remapper, t)
	}

}

func runTestMatchFloat(rule Rule, first, second string, vals []interface{}, remapper *FunctionRemap,
	t *testing.T, expect float64, priceindx int, symbol string){

	var err error
	rule.UpdateFormula = first + symbol + second
	vals, err = remapper.handleMatchFloat(vals, rule)
	if err != nil{
		t.Error("Unexpected error ", err)
	}
	if expect != vals[priceindx].(float64){
		t.Error("Calc mismatch, ", expect, " got ", vals[priceindx],
			" calc type ", symbol)
	}
}
func runTestMatchFloatLiteral(rule Rule, first string, second float64, vals []interface{}, remapper *FunctionRemap,
	t *testing.T, expect float64, priceindx int, symbol string){

	var err error
	rule.UpdateFormula = first + symbol + fmt.Sprintf("%f", second)
	vals, err = remapper.handleMatchFloat(vals, rule)
	if err != nil{
		t.Error("Unexpected error on literal ", err)
	}
	if expect != vals[priceindx].(float64){
		t.Error("Calc mismatch literal, ", expect, " got ", vals[priceindx])
	}
}

func Test_HandleMatchFloat(t *testing.T){

	vals, fields := getStdTestFields()
	remapper := newFunctionRemap()
	cfg := transform.TransformerCfg{
		Fields: fields,
	}
	remapper.Setup(&cfg)

	rule := Rule{
		UpdateField: "price",
		UpdateType:  CMPTYPE_FLOAT,
	}
	priceindx := findIndex(fields, "price")
	cases, first, second, firstindx, secondindx := getStdCases(fields)

	for _, tcase := range cases{
		vals[firstindx] = tcase.first
		vals[secondindx] = tcase.second
		expect := tcase.first + tcase.second
		runTestMatchFloat(rule, first, second, vals, remapper, t, expect, priceindx, "+")
		expect = tcase.first - tcase.second
		runTestMatchFloat(rule, first, second, vals, remapper, t, expect, priceindx, " -")
		expect = tcase.first * tcase.second
		runTestMatchFloat(rule, first, second, vals, remapper, t, expect, priceindx, " *  ")
		expect = tcase.first / tcase.second
		runTestMatchFloat(rule, first, second, vals, remapper, t, expect, priceindx, "   /  ")
		expect = tcase.first + tcase.second
		runTestMatchFloat(rule, first, second, vals, remapper, t, expect, priceindx, "+")
		expect = tcase.first - tcase.second
		runTestMatchFloat(rule, first, second, vals, remapper, t, expect, priceindx, " -")
		expect = tcase.first * tcase.second
		runTestMatchFloat(rule, first, second, vals, remapper, t, expect, priceindx, " *  ")
		expect = tcase.first / tcase.second
		runTestMatchFloat(rule, first, second, vals, remapper, t, expect, priceindx, "   /  ")

		expect = tcase.first + tcase.second
		runTestMatchFloatLiteral(rule, first, tcase.second, vals, remapper, t, expect, priceindx, "+")
		expect = tcase.first - tcase.second
		runTestMatchFloatLiteral(rule, first, tcase.second, vals, remapper, t, expect, priceindx, " -")
		expect = tcase.first * tcase.second
		runTestMatchFloatLiteral(rule, first, tcase.second, vals, remapper, t, expect, priceindx, " *  ")
		expect = tcase.first / tcase.second
		runTestMatchFloatLiteral(rule, first, tcase.second, vals, remapper, t, expect, priceindx, "   /  ")
		expect = tcase.first + tcase.second
		runTestMatchFloatLiteral(rule, first, tcase.second, vals, remapper, t, expect, priceindx, "+")
		expect = tcase.first - tcase.second
		runTestMatchFloatLiteral(rule, first, tcase.second, vals, remapper, t, expect, priceindx, " -")
		expect = tcase.first * tcase.second
		runTestMatchFloatLiteral(rule, first, tcase.second, vals, remapper, t, expect, priceindx, " *  ")
		expect = tcase.first / tcase.second
		runTestMatchFloatLiteral(rule, first, tcase.second, vals, remapper, t, expect, priceindx, "   /  ")
	}

}

func runTestMatchString(rule Rule, first, second string, vals []interface{}, remapper *FunctionRemap,
	t *testing.T, expect string, indx int, symbol string){

	var err error
	rule.UpdateFormula = first + symbol + second
	vals, err = remapper.handleMatchString(vals, rule)
	if err != nil{
		t.Error("Unexpected error ", err)
	}
	if expect != vals[indx].(string){
		t.Error("Calc mismatch, ", expect, " got ", vals[indx],
			" calc type ", symbol)
	}
}
func runTestMatchStringLiteral(rule Rule, first, second string, vals []interface{}, remapper *FunctionRemap,
	t *testing.T, expect string, indx int, symbol string){

	var err error
	rule.UpdateFormula = first + symbol + "'" + second + "'"
	vals, err = remapper.handleMatchString(vals, rule)
	if err != nil{
		t.Error("Unexpected error on literal ", err)
	}
	if expect != vals[indx].(string){
		t.Error("Calc mismatch literal, ", expect, " got ", vals[indx])
	}
}
func Test_HandleMatchString(t *testing.T) {

	vals, fields := getStdTestFields()
	remapper := newFunctionRemap()
	cfg := transform.TransformerCfg{
		Fields: fields,
	}
	remapper.Setup(&cfg)

	rule := Rule{
		UpdateField: "fruit",
		UpdateType:  CMPTYPE_STRING,
	}
	fruitindx := findIndex(fields, "fruit")
	cases, first, second, firstindx, secondindx := getStdCases(fields)

	for _, tcase := range cases{
		vals[firstindx] = tcase.firststr
		vals[secondindx] = tcase.secondstr
		expect := tcase.firststr + tcase.secondstr
		runTestMatchString(rule, first, second, vals, remapper, t, expect, fruitindx, "+")
		runTestMatchStringLiteral(rule, first, tcase.secondstr, vals, remapper, t, expect, fruitindx, "+")
	}
}
