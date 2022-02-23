package rename

import (
	"csv-sql/transform"
	"errors"
)

type RemapCfg map[string]string

type RemapTransform struct{
	remapping RemapCfg
}

func (p *RemapTransform) Setup(cfg *transform.TransformerCfg) {
}

func (p *RemapTransform) Do(vals []interface{}) ([]interface{}, error) {
	for i, val := range vals{
		switch val.(type) {
		case string:
			newval, found := p.remapping[val.(string)]
			if found{
				vals[i] = newval
			}
		default:
			return nil, errors.New("Only support rename strings currently")
		}
	}
	return vals, nil
}

func NewRemapping(remapping RemapCfg)transform.Transformer{
	return &RemapTransform{
		remapping: remapping,
	}
}