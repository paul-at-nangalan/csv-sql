package filter

import (
	"csv-sql/transform/shared"
	"errors"
	"fmt"
)

type FilterTransform struct{
	fields []string
	filter shared.FilterCfg
}

func NewFilterTransform(filter shared.FilterCfg) *FilterTransform {
	return &FilterTransform{filter: filter}
}


func (p *FilterTransform) Setup(cfg *shared.TransformerCfg) {
	p.fields = cfg.Fields
}

func (p *FilterTransform) Do(vals []interface{}) ([]interface{}, error) {
	if len(p.fields) == 0{
		return vals, nil
	}
	retvals := make([]interface{}, 0, len(vals))
	for i, val := range vals {
		filterin, found := p.filter.Filter[p.fields[i]]
		if !found {
			if !p.filter.DefaultFilterIn && !p.filter.DefaultFilterOut {
				return nil, errors.New(fmt.Sprint("Field ", p.fields[i],
					" not found and no defaults set, set either DefaultFilterIn or DefaultFilterOut"))
			}else if p.filter.DefaultFilterIn{
				retvals = append(retvals, val)
			}
		}else{
			if filterin{
				retvals = append(retvals, val)
			}
		}
	}
	return retvals, nil
}

