package main

import (
	"csv-sql/processor"
	"csv-sql/transform"
	"csv-sql/writer"
	"database/sql"
	"flag"
	"fmt"
	"github.com/paul-at-nangalan/csv-stuff/list"
	"github.com/paul-at-nangalan/json-config/cfg"
	"github.com/paul-at-nangalan/db-util/connect"
	"os"
)

const(
	DBTYPE_POSTGRES = "postgres"
	DBTYPE_MYSQL = "mysql" ////NEEDS TESTING
)

func main(){

	cfgdir := ""
	dbtype := "postgres"
	ignoredupes := true
	infile := ""
	table := ""
	ondupkeyclause := ""
	headerindex := 0
	flag.StringVar(&cfgdir, "cfg", "./cfg", "Config dir")
	flag.StringVar(&dbtype, "db", "postgres",
		fmt.Sprint("Databnase type, ", DBTYPE_POSTGRES, ",",
			DBTYPE_MYSQL))
	flag.BoolVar(&ignoredupes, "ignore-duplicates", true,
		"Ignore duplicates when inserting data")
	flag.StringVar(&infile, "infile", "", "Input csv file")
	flag.StringVar(&table, "table", "", "Table to insert data into")
	flag.StringVar(&ondupkeyclause, "on-dup-key-clause", "",
		"By default duplicate keys are ignored. If you need to update specific fields, use this clause to over ride that default behaviour")
	flag.IntVar(&headerindex, "header-row", 0, "Zero based index of the header row")
	flag.Parse()
	cfg.Setup(cfgdir)

	dbargtype := writer.ARGTYPE_NUMBERED
	dbignoredups := ""
	var db *sql.DB

	switch dbtype {
	case DBTYPE_MYSQL:
		dbargtype = writer.ARGTYPE_SIMPLE
		if ignoredupes {
			dbignoredups = "ON DUPLICATE KEY IGNORE"
		}
		/// TODO: db = connect to mysql
	case DBTYPE_POSTGRES:
		if ignoredupes{
			dbignoredups = "ON CONFLICT DO NOTHING"
		}
		db = connect.Connect()
	}
	if ondupkeyclause != ""{
		dbignoredups = ondupkeyclause
	}
	combinedfilter := transform.NewCombiner()
	proc := processor.NewProc(db, combinedfilter, dbargtype, dbignoredups)

	csvcfg := list.Config{
		HeaderRowIndex: headerindex,
	}
	err := proc.Process(infile, csvcfg, table)
	if err != nil{
		fmt.Println("Failed to process file, error ", err)
		os.Exit(1)
	}
}
