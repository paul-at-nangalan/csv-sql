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
	db             *sql.DB
	combinedfilter *transform.Combiner
	dbargtype writer.ArgType
	dbonduplicate string
}

func NewProc(db *sql.DB, combiner *transform.Combiner, dbargtype writer.ArgType, onduplicate string) *Proc {
	return &Proc{
		db:             db,
		combinedfilter: combiner,
		dbargtype: dbargtype,
		dbonduplicate: onduplicate,
	}
}

func (p *Proc)Process(filename string, cfg list.Config, tablename string)error{
	f, err := os.Open(filename)
	handlers.PanicOnError(err)
	defer f.Close()

	csvreader := csv.NewReader(f)
	csvreader.FieldsPerRecord = -1 /// Don't check number of records per line
	importer := list.NewCsvImporter(cfg, csvreader)

	datastore := data.NewMemStore()
	err = importer.Import(datastore)
	handlers.PanicOnError(err)

	fields := datastore.GetFields()
	header := make([]interface{}, len(fields))
	for i := 0; i < len(header); i++{
		header[i] = fields[i].Name()
	}
	newheader, err := p.combinedfilter.DoHeader(header)
	if err != nil{
		fmt.Println("Failed to parse header. Error ", err)
		return err
	}
	colnames := make([]string, len(newheader))
	for i, colname := range newheader {
		colnames[i] = colname.(string)
	}

	dbwriter := writer.NewDBWriter(p.db, tablename, colnames, p.dbargtype, p.dbonduplicate)

	bar := ""
	barstep := ">"
	for i := 0; ; i++{
		rowdata, valid := datastore.GetRow(int64(i))
		if !valid{
			break
		}
		row, err := p.combinedfilter.DoData(rowdata.GetRow())
		if err != nil{
			fmt.Println("ERROR on row: ", i)
			return err
		}
		bar += barstep
		if len(bar) > 99{
			bar = ""
		}
		fmt.Printf("\r[%-100s] %d", bar, i)
		//fmt.Println("Row: ", row)
		_, err = dbwriter.InsRow(row...)
		if err != nil{
			return err
		}
	}
	return nil
}

