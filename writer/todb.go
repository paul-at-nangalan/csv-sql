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

///Do this so we can switch in a write to file DB interface
type DB interface {
	Prepare(query string) (*sql.Stmt, error)
}

type DbWriter struct{
	db DB
	insstmt *sql.Stmt
}

///onduplicate can be
// - empty - duplicates should generate an error
// - a valid ignore statement for the DB type, e.g. ON DUPLICATE DO NOTHING
func NewDBWriter(db DB, tablename string, colnames []string, argtype ArgType, onduplicate string)*DbWriter{
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
			arg = fmt.Sprintf("$%d", (i + 1))
		}
		inssql += sep + arg
		sep = ","
	}
	inssql += ") " + onduplicate
	fmt.Println("Insert statement: ", inssql)
	insstmt, err := db.Prepare(inssql)
	handlers.PanicOnError(err)

	return &DbWriter{
		db: db,
		insstmt: insstmt,
	}
}
////keep it simple for now ... no bulk updates
func (p *DbWriter)InsRow(vals ...interface{})(sql.Result, error){
	return p.insstmt.Exec(vals...)
}
