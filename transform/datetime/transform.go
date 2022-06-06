package datetime

import (
	"csv-sql/transform/shared"
	"github.com/paul-at-nangalan/csv-stuff/data"
	"time"
)

type DatetimeTransform struct{
	fieldindexes map[string]int
	cfg          shared.Config
}

func NewDatetimeTransform(cfg shared.Config)shared.Transformer{
	return &DatetimeTransform{
		cfg: cfg,
		fieldindexes: make(map[string]int),
	}
}

func (p *DatetimeTransform) Setup(cfg *shared.TransformerCfg) {
	for i, field := range cfg.Fields{
		p.fieldindexes[field] = i
	}
}

func (p *DatetimeTransform) Do(vals []interface{}) ([]interface{}, error) {
	datarow := data.NewDataRow(vals, p.fieldindexes)
	for _, tx := range p.cfg.Mapping{
		val, err := datarow.Get(tx.Fieldname)
		if err != nil{
			return nil, err
		}
		if val == "" && tx.AllowNil{
			datarow.Set(tx.Fieldname, nil)
			continue
		}
		t, err := time.Parse(tx.From, val.(string))
		if err != nil{
			return nil, err
		}
		ntxval := t.Format(tx.To)
		datarow.Set(tx.Fieldname, ntxval)
	}
	return vals, nil
}

