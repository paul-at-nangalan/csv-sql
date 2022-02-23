package shared

import (
	"strings"
)

type HeaderTransforms struct {
	AddCols    CfgAddColumn
	RenameCols RemapCfg
}

type DataTransforms struct {
	AddData      CfgAddColumn
	RenameData   RemapCfg
	DatetimeData Config
	FunctionData FunctionMapCfg
}

type TransformerCfg struct {
	///Original col names from csv
	Fields []string

	Headers HeaderTransforms
	Data    DataTransforms
}

func (p *TransformerCfg) Expand() {
}

type Transformer interface {
	Setup(cfg *TransformerCfg)
	Do([]interface{}) ([]interface{}, error)
}

type CfgAddColumn struct {
	Values []interface{}
}

func (c *CfgAddColumn) Expand() {
}

type DatetimeCfg struct {
	From      string
	To        string
	Fieldname string
}

type Config struct {
	Mapping []DatetimeCfg
}

func (p *Config) Expand() {
}

/** rules:
	 where col = value  set col = newvalue
	 e.g. price: "where fieldname = `GBp` set price = price * 0.01

	RuleType: e.g. where
	Clause e.g. fieldname = 'GBp'
				/// price < 1.233
				/// strings must be quoted with '
				/// float values must start with a numeric [0-9]
				/// all other values will be treated as fields
	ComparisonType: float or string
	AcceptDeviation: mainly for float comparison, if set, a float comparision will be
						//// considered equal if
						////  abs(v1 - v2) < v1 * AcceptDeviation
	UpdateField: The field name to be updated if the clause is true
	UpdateFormula: e.g. price * 0.01, where price is a field name
	UpdateType: string or float
 */
type Rule struct {
	RuleType string /** e.g. where */
	Clause   string /// e.g. fieldname = 'GBp'
	/// price < 1.233
	/// strings must be quoted with '
	/// float values must start with a numeric [0-9]
	/// all other values will be treated as fields
	ComparisonType  string  /// options are float, string
	AcceptDeviation float64 //// mainly for float comparison, if set, a float comparision will be
	//// considered equal if
	////  abs(v1 - v2) < v1 * AcceptDeviation
	UpdateField   string /// e.g. price
	UpdateFormula string /// e.g. price * 0.01
	UpdateType    string /// options are float, string
}

type FunctionMapCfg struct {
	FieldToRule []Rule ///map Field Name to a rule
}

func (p *FunctionMapCfg) Expand() {
	for _, rule := range p.FieldToRule {
		rule.RuleType = strings.TrimSpace(rule.RuleType)
		rule.RuleType = strings.ToLower(rule.RuleType)
		rule.ComparisonType = strings.TrimSpace(rule.ComparisonType)
		rule.ComparisonType = strings.ToLower(rule.ComparisonType)
	}
}

type RemapCfg map[string]string

