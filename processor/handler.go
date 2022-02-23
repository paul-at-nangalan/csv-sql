package processor

import (
	"csv-sql/transform"
	"csv-sql/writer"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/paul-at-nangalan/csv-stuff/data"
	"github.com/paul-at-nangalan/csv-stuff/list"
	"github.com/paul-at-nangalan/errorhandler/handlers"
	"os"
)

type Proc struct{
	db *sql.DB
	combiner *transform.Combiner
}

func NewProc(db *sql.DB, combiner *transform.Combiner) *Proc {
	return &Proc{
		db: db,
		combiner: combiner,
	}
}

func (p *Proc)Process(filename string, cfg list.Config)error{
	f, err := os.Open(filename)
	handlers.PanicOnError(err)
	defer f.Close()

	csvreader := csv.NewReader(f)
	importer := list.NewCsvImporter(cfg, csvreader)

	datastore := data.NewMemStore()
	err = importer.Import(datastore)
	handlers.PanicOnError(err)

	fields := datastore.GetFields()
	header := make([]interface{}, len(fields))
	for i := 0; i < len(header); i++{
		header[i] = fields[i].Name()
	}
	newheader, err := p.combiner.DoHeader(header)
	if err != nil{
		fmt.Println("Failed to parse header. Error ", err)
		return err
	}
}

