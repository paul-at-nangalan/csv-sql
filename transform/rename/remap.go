package rename

import (
	"csv-sql/transform/shared"
	"fmt"
)

type RemapTransform struct{
	remapping shared.RemapCfg
}

func (p *RemapTransform) Setup(cfg *shared.TransformerCfg) {
}

func (p *RemapTransform) Do(vals []interface{}) ([]interface{}, error) {
	for i, val := range vals{
		switch val.(type) {
		case string:
			newval, found := p.remapping[val.(string)]
			if found{
				vals[i] = newval
				fmt.Println("Rename ", val, " to ", vals[i])
			}
		}
	}
	return vals, nil
}

func NewRemapping(remapping shared.RemapCfg)shared.Transformer{
	return &RemapTransform{
		remapping: remapping,
	}
}