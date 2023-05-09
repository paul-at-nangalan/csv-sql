package cast

import (
	"csv-sql/transform/shared"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const(
	TOFLOAT = "float"
	TOINT = "int"
	TOFLOATZERO = "float_or_zero"
	TOINTZERO = "int_or_zero"
)

type Cast struct{
	cfg shared.CastCfg
	casts []string
}

func NewCast(cfg shared.CastCfg)shared.Transformer{
	return &Cast{
		cfg: cfg,
	}
}

func (p *Cast) Setup(cfg *shared.TransformerCfg) {
	p.casts = make([]string, len(cfg.Fields))
	for i, field := range cfg.Fields{
		casttype, ok := p.cfg[field]
		if ok{
			fmt.Println("Cast ", field, " to ", casttype)
			p.casts[i] = casttype
		}
	}
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
		if parsed == ""{
			return 0, nil
		}
		factor := float64(1)
		if strings.HasSuffix(parsed, "%") {
			factor = 0.01
			parsed = strings.Replace(parsed, "%", "", -1)

		}
		parsed = strings.Replace(parsed, ",", "", -1)
		f, err := strconv.ParseFloat(parsed, 64)
		if err != nil{
			return 0, err
		}
		return f * factor, nil
	case nil:
		return 0, nil
	}
	return 0, errors.New(fmt.Sprint("Failed to convert", v, " to float"))
}

func toInt(v interface{})(int64, error){
	switch i := v.(type) {
	case int64:
		return i, nil
	case int32:
		return int64(i), nil
	case float64:
		return int64(i), nil
	case float32:
		return int64(i), nil
	case int:
		return int64(i), nil
	case uint64:
		return int64(i), nil
	case uint32:
		return int64(i), nil
	case uint:
		return int64(i), nil
	case string:
		parsed := strings.TrimSpace(i)
		if parsed == ""{
			return 0, nil
		}
		parsed = strings.Replace(parsed, ",", "", -1)
		return strconv.ParseInt(parsed, 10, 64)
	case nil:
		return 0, nil
	}
	return 0, errors.New(fmt.Sprint("Failed to convert", v, " to int"))
}

func (p *Cast) Do(vals []interface{}) ([]interface{}, error) {
	for i, val := range vals{
		switch p.casts[i] {
		case TOFLOAT:
			//fmt.Println("Format ", vals[i], " to ", p.casts[i])
			fval, err := toFloat(val)
			if err != nil {
				return nil, err
			}
			vals[i] = fval
		case TOINT:
			fval, err := toInt(val)
			if err != nil {
				return nil, err
			}
			vals[i] = fval
		case TOFLOATZERO:
			//fmt.Println("Format ", vals[i], " to ", p.casts[i])
			fval, _ := toFloat(val)
			vals[i] = fval
		case TOINTZERO:
			fval, _ := toInt(val)
			vals[i] = fval
		}
	}
	return vals, nil
}

