package addcol

import (
	"csv-sql/transform"
	_ "github.com/paul-at-nangalan/json-config/cfg"
)

type CfgAddColumn struct{
	Values []interface{}
}

func (c *CfgAddColumn) Expand() {
}

type AddColumn struct{
	values []interface{}
}

func NewAddColumn(cfg CfgAddColumn)transform.Transformer{
	return &AddColumn{
		values: cfg.Values,
	}
}

func (p *AddColumn) Setup(cfg *transform.TransformerCfg) {
}

func (p *AddColumn) Do(vals []interface{}) ([]interface{}, error) {
	vals = append(vals, p.values...)
	return vals, nil
}

