package trimspace

import (
	"csv-sql/transform/shared"
	"strings"
)

type TrimSpace struct{
	fields []string
	config shared.TrimSpaceCfg
}

func NewTrimspaceTransform(cfg shared.TrimSpaceCfg) *TrimSpace {
	return &TrimSpace{config: cfg}
}

func (p *TrimSpace) Setup(cfg *shared.TransformerCfg) {
	p.fields = cfg.Fields
}

func (p *TrimSpace) Do(vals []interface{}) ([]interface{}, error) {
	for i, val := range vals{
		if ok, found := p.config.Fields[p.fields[i]]; ok && found{
			vals[i] = strings.TrimSpace(val.(string))
		}
	}
	return vals, nil
}
