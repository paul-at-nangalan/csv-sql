package addcol

import (
	"csv-sql/transform/shared"
	_ "github.com/paul-at-nangalan/json-config/cfg"
)

type AddColumn struct{
	values []interface{}
}

func NewAddColumn(cfg shared.CfgAddColumn)shared.Transformer{
	return &AddColumn{
		values: cfg.Values,
	}
}

func (p *AddColumn) Setup(cfg *shared.TransformerCfg) {
}

func (p *AddColumn) Do(vals []interface{}) ([]interface{}, error) {
	vals = append(vals, p.values...)
	return vals, nil
}

