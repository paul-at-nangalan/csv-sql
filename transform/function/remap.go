package function

import (
	"csv-sql/transform"
	"fmt"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

const(
	RULETYPE_WHERE = "where"
	CMPTYPE_FLOAT = "float"
	CMPTYPE_STRING = "string"

	CMPTYPE_L = "<"
	CMPTYPE_G = ">"
	CMPTYPE_LE = "<="
	CMPTYPE_GE = ">="
	CMPTYPE_E = "="
	CMPTYPE_NE = "!="
)
////rules:
	/// where col = value  set col = newvalue
	/// e.g. price: "where fieldname = `GBp` set price = price * 0.01
type Rule struct{
	RuleType string /// e.g. where
	Clause string /// e.g. fieldname = 'GBp'
	ComparisonType string /// options are float, string
	AcceptDeviation float64 //// mainly for float comparison, if set, a float comparision will be
							//// considered equal if
							////  abs(v1 - v2) < v1 * AcceptDeviation
	Set string /// e.g. price = price * 0.01
}

type FunctionMapCfg struct{
	FieldToRule []Rule ///map Field Name to a rule
}

func (p *FunctionMapCfg)Expand(){
	for _, rule := range p.FieldToRule{
		rule.RuleType = strings.TrimSpace(rule.RuleType)
		rule.RuleType = strings.ToLower(rule.RuleType)
		rule.ComparisonType = strings.TrimSpace(rule.ComparisonType)
		rule.ComparisonType = strings.ToLower(rule.ComparisonType)
	}
}

type FunctionRemap struct{
	rules FunctionMapCfg
	fieldindexes map[string]int

	matchclause *regexp.Regexp
}

func NewFunctionRemap(cfg FunctionMapCfg)transform.Transformer{
	match := regexp.MustCompile("([a-zA-Z0-9_]*)[\\s]*([=!<>]{1,})[\\s]*(.*)")
	return &FunctionRemap{
		rules: cfg,
		fieldindexes: make(map[string]int),
		matchclause: match,
	}
}

func (p *FunctionRemap) Setup(cfg transform.TransformerCfg) {
	for i, field := range cfg.Fields{
		p.fieldindexes[field] = i
	}
}

func (p *FunctionRemap)getFieldVal(fieldname string, vals []interface{})(val interface{}, err error){

	cmpindex, exists := p.fieldindexes[fieldname]
	if !exists{
		return nil, NewInvalidFieldName(fieldname)
	}
	cmpval := vals[cmpindex]
	return cmpval, nil
}

func toFloat(v interface{})(float64, error){
	switch i := v.(type) {
	case float64:
		return i, nil
	case float32:
		return float64(i), nil
	case int64:
		return float64(i), nil
	case int32:
		return float64(i), nil
	case int:
		return float64(i), nil
	case uint64:
		return float64(i), nil
	case uint32:
		return float64(i), nil
	case uint:
		return float64(i), nil
	case string:
		return strconv.ParseFloat(i, 64)
	}
	return 0, NewInvalidValue(v)
}

func (p *FunctionRemap)cmpAsFloats(v1, v2 interface{})(float64, error){
	fval1, err := toFloat(v1)
	if err != nil{
		return 0, err
	}
	fval2, err := toFloat(v2)
	if err != nil{
		return 0, err
	}
	return fval1 - fval2, nil
}

func (p *FunctionRemap)cmpAsString(v1, v2 interface{})(int, error){
	strval1, ok := v1.(string)
	if !ok {
		return 0, NewInvalidString(v1)
	}
	strval2, ok := v2.(string)
	if !ok{
		return 0, NewInvalidString(v2)
	}
	diff := strings.Compare(strval1, strval2)
	return diff, nil
}

func (p *FunctionRemap)handleWhere(vals []interface{}, rule Rule)(ismatched bool, err error){
	clauseparts := p.matchclause.FindStringSubmatch(rule.Clause)
	if len(clauseparts) < 3{
		return false, NewInvalidClause(rule.Clause, "Expected <field name> {=|>|<|>=|<=} {<field name> | value}")
	}
	cmpfieldname := clauseparts[1]
	cmptype := clauseparts[2]
	switch cmptype {
	case CMPTYPE_E, CMPTYPE_NE, CMPTYPE_G, CMPTYPE_GE, CMPTYPE_L, CMPTYPE_LE:
		//// all good
	default:
		return false, NewInvalidClause(cmptype, ": must be one of =, !=, >, >=, <=, <")
	}
	comparitorfield := clauseparts[3]

	cmpval, err := p.getFieldVal(cmpfieldname, vals)
	if err != nil{
		return false, err
	}

	isnumeric := unicode.IsDigit(rune(comparitorfield[0]))
	isstring := false
	if rune(comparitorfield[0]) == '\''{
		isstring = true
	}
	var comparitorval interface{}
	if !isnumeric && !isstring{
		///this should be a field name
		comparitorval, err = p.getFieldVal(comparitorfield, vals)
		if err != nil{
			return false, err
		}
	}

	isequal := false
	islessthan := false
	switch rule.ComparisonType {
	case CMPTYPE_FLOAT:
		diff, err := p.cmpAsFloats(cmpval, comparitorval)
		if err != nil {
			fmt.Println("Float comparision failed for ", rule.Clause, ": ", err)
			return false, err
		}

		fval, err := toFloat(cmpval)
		handlers.PanicOnError(err)
		if math.Abs(diff) < (fval + (fval * rule.AcceptDeviation)){
			isequal  = true
		}
		if diff < 0{
			islessthan = true
		}
	case CMPTYPE_STRING:
		diff, err := p.cmpAsString(cmpval, comparitorval)
		if err != nil{
			fmt.Println("String comparision failed for ", rule.Clause, ": ", err)
			return false, err
		}
		if diff == 0{
			isequal = true
		}
		if diff < 0{
			islessthan = true
		}
	}
	ismatched := false
	if isequal && strings.Contains(rule.RuleType, "="){
		ismatched = true
	}else if islessthan && strings.Contains(rule.RuleType, "<"){
		ismatched = true
	}else if !islessthan && strings.Contains(rule.RuleType, ">"){
		ismatched = true
	}else if rule.RuleType == "!="{
		ismatched = true
	}
	return ismatched, nil
}


func (p *FunctionRemap) Do(vals []interface{}) ([]interface{}, error) {
	for _, rule := range p.rules.FieldToRule{
		if rule.RuleType == RULETYPE_WHERE{
			ismatched, err := p.handleWhere(vals, rule)
		}
	}
}

