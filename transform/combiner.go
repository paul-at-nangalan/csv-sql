package transform

type TransformerCfg struct{
	///Original col names from csv
	Fields []string
}

type Transformer interface {
	Setup(cfg TransformerCfg)
	Do([]interface{})([]interface{}, error)
}

type Combiner struct{
	transformers []Transformer
}

func NewCombiner()*Combiner{
	return &Combiner{
		transformers: make([]Transformer, 0),
	}
}

func (p *Combiner)Do(vals []interface{})([]interface{}, error){
	for _, transformer := range p.transformers{
		var err error
		vals, err = transformer.Do(vals)
		if err != nil{
			return nil, err
		}
	}
	return vals, nil
}

func (p *Combiner)Add(tx Transformer){
	p.transformers = append(p.transformers, tx)
}
