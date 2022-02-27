package transform

import (
	"csv-sql/transform/addcol"
	"csv-sql/transform/datetime"
	filter2 "csv-sql/transform/filter"
	"csv-sql/transform/function"
	"csv-sql/transform/rename"
	"csv-sql/transform/shared"
	"github.com/paul-at-nangalan/json-config/cfg"
)

/**
	In this version we add cols and rename cols as the first steps
	So any data transforms that are based on the col name, should use the
	_new_ col name (not the original)

	DoHeaders must be called at least once before DoData
 */
type Combiner struct{
	transformers []shared.Transformer
	headers []shared.Transformer

	txconf *shared.TransformerCfg
}

func NewCombinerWithConfig(txconf *shared.TransformerCfg)*Combiner{
	combiner := &Combiner{
		transformers: make([]shared.Transformer, 0),
		headers: make([]shared.Transformer, 0),
	}

	combiner.txconf = txconf
	renamer := rename.NewRemapping(combiner.txconf.Headers.RenameCols)
	addcols := addcol.NewAddColumn(combiner.txconf.Headers.AddCols)
	filter := filter2.NewFilterTransform(combiner.txconf.Data.Filter)
	combiner.headers = append(combiner.headers, renamer, addcols, filter)

	renamer = rename.NewRemapping(combiner.txconf.Data.RenameData)
	addcols = addcol.NewAddColumn(combiner.txconf.Data.AddData)
	function := function.NewFunctionRemap(combiner.txconf.Data.FunctionData)
	datetime := datetime.NewDatetimeTransform(combiner.txconf.Data.DatetimeData)
	combiner.transformers = append(combiner.transformers,
		addcols, function, datetime, renamer, filter)

	return combiner
}
func NewCombiner()*Combiner{
	txconf := &shared.TransformerCfg{}
	cfg.Read("transforms", txconf)
	return NewCombinerWithConfig(txconf)
}

func (p *Combiner)DoHeader(vals []interface{})([]interface{}, error){
	for _, header := range p.headers{
		var err error
		vals, err = header.Do(vals)
		if err != nil{
			return nil, err
		}
	}

	////Setup the data transforms based on the new headers
	p.txconf.Fields = make([]string, len(vals))
	for i, val := range vals {
		p.txconf.Fields[i] = val.(string)
	}
	for _, transformer := range p.transformers{
		transformer.Setup(p.txconf)
	}
	////Finally apply the filter to filter in/out only what we want
	/// The filter must be the last transform
	vals, err := p.headers[len(p.headers) - 1].Do(vals)
	if err != nil{
		return nil, err
	}
	return vals, nil
}

func (p *Combiner)DoData(vals []interface{})([]interface{}, error){
	for _, transformer := range p.transformers{
		var err error
		vals, err = transformer.Do(vals)
		if err != nil{
			return nil, err
		}
	}
	return vals, nil
}

func (p *Combiner)add(tx shared.Transformer){
	p.transformers = append(p.transformers, tx)
}
