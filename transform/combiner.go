package transform

import (
	"csv-sql/transform/addcol"
	"csv-sql/transform/datetime"
	"csv-sql/transform/function"
	"csv-sql/transform/rename"
	"github.com/paul-at-nangalan/json-config/cfg"
)

type HeaderTransforms struct{
	AddCols addcol.CfgAddColumn
	RenameCols rename.RemapCfg
}

type DataTransforms struct{
	AddData addcol.CfgAddColumn
	RenameData rename.RemapCfg
	DatetimeData datetime.Config
	FunctionData function.FunctionMapCfg
}

type TransformerCfg struct{
	///Original col names from csv
	Fields []string

	Headers HeaderTransforms
	Data DataTransforms
}

func (p *TransformerCfg) Expand() {
}

type Transformer interface {
	Setup(cfg *TransformerCfg)
	Do([]interface{})([]interface{}, error)
}

/**
	In this version we add cols and rename cols as the first steps
	So any data transforms that are based on the col name, should use the
	_new_ col name (not the original)

	DoHeaders must be called at least once before DoData
 */
type Combiner struct{
	transformers []Transformer
	headers []Transformer

	txconf *TransformerCfg
}

func NewCombinerWithConfig(txconf *TransformerCfg)*Combiner{
	combiner := &Combiner{
		transformers: make([]Transformer, 0),
		headers: make([]Transformer, 0),
	}

	combiner.txconf = txconf
	renamer := rename.NewRemapping(combiner.txconf.Headers.RenameCols)
	addcols := addcol.NewAddColumn(combiner.txconf.Headers.AddCols)
	combiner.headers = append(combiner.headers, renamer, addcols)

	renamer = rename.NewRemapping(combiner.txconf.Data.RenameData)
	addcols = addcol.NewAddColumn(combiner.txconf.Data.AddData)
	function := function.NewFunctionRemap(combiner.txconf.Data.FunctionData)
	datetime := datetime.NewDatetimeTransform(combiner.txconf.Data.DatetimeData)
	combiner.transformers = append(combiner.transformers,
		addcols, function, datetime, renamer)

	return combiner
}
func NewCombiner()*Combiner{
	txconf := &TransformerCfg{}
	cfg.Read("transforms", txconf)
	return NewCombinerWithConfig(txconf)
}

func (p *Combiner)DoHeader(vals []interface{})([]interface{}, error){
	for _, headers := range p.headers{
		var err error
		vals, err = headers.Do(vals)
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

func (p *Combiner)add(tx Transformer){
	p.transformers = append(p.transformers, tx)
}
