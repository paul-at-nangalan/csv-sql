package writer

import (
	"database/sql"
	"fmt"
	"github.com/paul-at-nangalan/errorhandler/handlers"
)

type ArgType string
const(
	///just support these for now
	ARGTYPE_SIMPLE ArgType = "?"
	ARGTYPE_NUMBERED ArgType = "$x"
)

type DbWriter struct{
	db *sql.DB
	insstmt *sql.Stmt
}

func NewDBWriter(db *sql.DB, tablename string, colnames []string, argtype ArgType)*DbWriter{
	inssql := `INSERT INTO ` + tablename + ` (`

	sep := ""
	for _, col := range colnames{
		inssql += sep + col
		sep = ","
	}
	inssql += ") VALUES ("
	sep = ""
	for i, _ := range colnames{
		arg := "?"
		if argtype == ARGTYPE_NUMBERED{
			arg = fmt.Sprintf("$%d", i)
		}
		inssql += arg
		sep = ","
	}
	inssql += ")"
	insstmt, err := db.Prepare(inssql)
	handlers.PanicOnError(err)

	return &DbWriter{
		db: db,
		insstmt: insstmt,
	}
}
////keep it simple for now ... no bulk updates
func (p *DbWriter)InsRow(vals ...interface{})(sql.Result, error){
	return p.insstmt.Exec(vals)
}
