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
		parsed = strings.Replace(parsed, ",", "", -1)
		return strconv.ParseFloat(parsed, 64)
	}
	return 0, errors.New(fmt.Sprint("Failed to convert", v, " to float"))
}

func (p *Cast) Do(vals []interface{}) ([]interface{}, error) {
	for i, val := range vals{
		if p.casts[i] == TOFLOAT{
			fmt.Println("Format ", vals[i], " to ", p.casts[i])
			fval, err := toFloat(val)
			if err != nil{
				return nil, err
			}
			vals[i] = fval
		}
	}
	return vals, nil
}

