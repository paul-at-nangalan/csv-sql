package function

import (
	"csv-sql/transform/shared"
	"fmt"
	"github.com/Knetic/govaluate"
	"github.com/paul-at-nangalan/csv-stuff/data"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

const(
	RULETYPE_WHERE = "where"
	CMPTYPE_FLOAT = "float"
	CMPTYPE_STRING = "string"

	RULETYPE_FUNC = "func"
	FUNCTYPE_EXPENV = "expand-env"

	CMPTYPE_L = "<"
	CMPTYPE_G = ">"
	CMPTYPE_LE = "<="
	CMPTYPE_GE = ">="
	CMPTYPE_E = "="
	CMPTYPE_NE = "!="
)

type FunctionRemap struct{
	rules        shared.FunctionMapCfg
	fieldindexes map[string]int

	matchclause *regexp.Regexp
}

func NewFunctionRemap(cfg shared.FunctionMapCfg)shared.Transformer{
	match := regexp.MustCompile("([a-zA-Z0-9_]*)[\\s]*([=!<>]{1,})[\\s]*(.*)")
	return &FunctionRemap{
		rules: cfg,
		fieldindexes: make(map[string]int),
		matchclause: match,
	}
}

func (p *FunctionRemap) Setup(cfg *shared.TransformerCfg) {
	for i, field := range cfg.Fields{
		p.fieldindexes[field] = i
	}
}

func (p *FunctionRemap)getFieldVal(fieldname string, vals []interface{})(val interface{}, err error){
	datarow := data.NewDataRow(vals, p.fieldindexes)
	return datarow.Get(fieldname)
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
		parsed := strings.TrimSpace(i)
		parsed = strings.Replace(parsed, ",", "", -1)
		return strconv.ParseFloat(parsed, 64)
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

func (p *FunctionRemap)handleWhere(vals []interface{}, rule shared.Rule)(ismatched bool, err error){
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
		//fmt.Println("Get comparitor field ", comparitorfield)
		comparitorval, err = p.getFieldVal(comparitorfield, vals)
		if err != nil{
			return false, err
		}
	}else{
		comparitorval = comparitorfield
		if isstring{
			comparitorval = strings.Trim(comparitorval.(string), "'")
		}
	}

	isequal := false
	islessthan := false
	switch rule.ComparisonType {
	case CMPTYPE_FLOAT:
		//fmt.Println("Compare as float, cmpval: ", cmpval, " comparitor: ", comparitorval)
		diff, err := p.cmpAsFloats(cmpval, comparitorval)
		if err != nil {
			fmt.Println("Float comparision failed for ", rule.Clause, ": ", err)
			return false, err
		}
		//fmt.Println("Diff is ", diff)
		fval, err := toFloat(cmpval)
		handlers.PanicOnError(err)
		if math.Abs(diff) <= (fval * rule.AcceptDeviation){
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
	////Using switch statements to avoid long if statements
	switch cmptype {
	case "<=", ">=", "=":
		if isequal{
			ismatched = true
		}
	case "!=":
		if !isequal{
			ismatched = true
		}
	}

	switch cmptype {
	case "<","<=":
		//// equality is already checked
		if islessthan{
			ismatched = true
		}
	case ">",">=":
		//// equality is already checked
		if !islessthan && !isequal{
			ismatched = true
		}
	}
	return ismatched, nil
}

func (p *FunctionRemap)handleMatchFloat(vals []interface{}, rule shared.Rule)([]interface{}, error){
	/// fieldname
	///
	setindex, found := p.fieldindexes[rule.UpdateField]
	if !found{
		return nil, NewInvalidFieldName(rule.UpdateField)
	}

	//fmt.Println("Update formula: ", rule.UpdateFormula)
	valuate, err := govaluate.NewEvaluableExpression(rule.UpdateFormula)
	if err != nil{
		return nil,NewInvalidExpression(rule.UpdateFormula, err)
	}
	paramhandler := &EvalParams{
		datarow: data.NewDataRow(vals, p.fieldindexes),
	}
	calcval, err := valuate.Eval(paramhandler)
	if err != nil{
		return nil, NewInvalidExpression(rule.UpdateFormula, err)
	}
	//fmt.Println("Calc value is ", calcval)
	vals[setindex] = calcval
	return vals, nil
}

func (p *FunctionRemap)handleMatchString(vals []interface{}, rule shared.Rule)([]interface{}, error){
	setindex, found := p.fieldindexes[rule.UpdateField]
	if !found{
		return nil, NewInvalidFieldName(rule.UpdateField)
	}
	inquote := false
	lastchar := rune(' ')
	fields := strings.FieldsFunc(rule.UpdateFormula, func(r rune)bool{
		ret := false
		if !inquote{
			if r == '\'' {
				inquote = true
				ret = true
			}
			if r == ' ' || r == '+'{
				ret = true
			}
		}else{
			if r == '\''{
				if lastchar == '\\'{
					ret = false
				}
				inquote = false
				ret = true
			}
		}
		lastchar = r
		return ret
	})
	strval := ""
	for _, rawfield := range fields{
		field := strings.TrimSpace(rawfield)
		indx, found := p.fieldindexes[field]
		sep := ""
		if found{
			val := vals[indx]
			strval += fmt.Sprint(sep, val)
		}else{
			strval += sep + rawfield
		}
	}
	vals[setindex] = strval
	return vals, nil
}

type EvalParams struct{
	datarow *data.DataRow
}
//// For use by govaluate
func (p *EvalParams) Get(name string) (interface{}, error) {
	field := strings.TrimSpace(name)

	val, err := p.datarow.Get(field)
	if err != nil{
		return nil, err
	}
	fval, err := toFloat(val)
	if err != nil {
		return nil, err
	}
	//fmt.Println("Got val:", fval)
	return fval, nil
}

func (p *FunctionRemap)expandEnv(vals []interface{})([]interface{}, error){
	for i, val := range vals{
		switch val.(type) {
		case string:
			vals[i] = os.ExpandEnv(val.(string))
		}
	}
	return vals, nil
}


func (p *FunctionRemap) Do(vals []interface{}) ([]interface{}, error) {
	for _, rule := range p.rules.FieldToRule{
		switch rule.RuleType{
		case RULETYPE_WHERE:
			ismatched, err := p.handleWhere(vals, rule)
			if err != nil{
				fmt.Println("ERROR: ", err)
				return nil, err
			}
			if ismatched && rule.UpdateType == CMPTYPE_FLOAT{
				vals, err = p.handleMatchFloat(vals, rule)
				if err != nil{
					fmt.Println("ERROR: ", err)
					return nil, err
				}
			} else if ismatched && rule.UpdateType == CMPTYPE_STRING{
				vals, err = p.handleMatchString(vals, rule)
				if err != nil{
					fmt.Println("ERROR: ", err)
					return nil, err
				}
			}
		case RULETYPE_FUNC:
			var err error
			switch rule.UpdateFormula{

			case FUNCTYPE_EXPENV:
				vals, err = p.expandEnv(vals)
				if err != nil{
					fmt.Println("ERROR: ", err)
					return nil, err
				}
			default:
				return nil, NewInvalidRule("UpdateFormula", rule.UpdateFormula)
			}
		default:
			return nil, NewInvalidRule("Ruletype", rule.RuleType)
		}
	}
	return vals, nil
}

