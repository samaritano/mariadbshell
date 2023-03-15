package main

import (
	"compress/gzip"
	"database/sql"
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func dump_table(targetdir string, host string, user string, pwd string, database string, table string) {
	log.Println("Start dump of table " + table)
	startTime := time.Now()
	//dumpFile, err := os.OpenFile(targetdir+"/"+database+"_"+table+".dmp", os.O_CREATE|os.O_WRONLY, 0666)
	dumpFile, err := os.OpenFile(targetdir+"/"+database+"_"+table+".dmp.gz", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer dumpFile.Close()

	gzw := gzip.NewWriter(dumpFile)
	defer gzw.Close()
	defer gzw.Flush()

	cmd := exec.Command("mysqldump", "-h"+host, "-u"+user, "-p"+pwd, database, table)
	//cmd.Stdout = dumpFile
	cmd.Stdout = gzw
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(startTime)
	log.Println("End dump of table " + table + "(" + elapsed.String() + ")")
}

func import_table(importfile string, host string, user string, pwd string, database string) {
	log.Println("Start import of file " + importfile)
	startTime := time.Now()

	dumpFile, err := os.OpenFile(importfile, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer dumpFile.Close()

	gzr, errGz := gzip.NewReader(dumpFile)
	if errGz != nil {
		log.Fatal(err)
	}
	defer gzr.Close()

	cmd := exec.Command("mysql", "-h"+host, "-u"+user, "-p"+pwd, "-A", database)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	//cmd.Stdin = dumpFile
	cmd.Stdin = gzr

	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(startTime)
	log.Println("End import of file " + importfile + "(" + elapsed.String() + ")")
}

func main() {
	logFilename := os.Args[1]
	serverName := os.Args[2]
	userName := os.Args[3]
	passWord := os.Args[4]
	dataBase := os.Args[5]
	targetDir := os.Args[6]
	numberChilds, err := strconv.Atoi(os.Args[7])
	if err != nil {
		log.Fatal(err)
	}
	action := os.Args[8]

	logFile, err := os.OpenFile(logFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(io.MultiWriter(logFile, os.Stdout))

	if action != "export" && action != "import" {
		log.Fatal("You passed '" + action + "' as last command line argument but it must be 'import' or 'export'")
	}

	if _, err := os.Stat(targetDir); errors.Is(err, os.ErrNotExist) {
		log.Println("Directory " + targetDir + " doesn't exists, creating it")
		err = os.Mkdir(targetDir, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
	}

	dir, _ := os.ReadDir(targetDir)
	if len(dir) > 0 && action == "export" {
		log.Fatal("Directory " + targetDir + " is not empty. Cleanup it or pass another directory as target dir")
	} else if len(dir) == 0 && action == "import" {
		log.Fatal("Directory " + targetDir + " is empty")
	}

	db, err := sql.Open("mysql", userName+":"+passWord+"@tcp("+serverName+")/"+dataBase)
	if err != nil {
		log.Fatal(err)
	}

	db.SetConnMaxLifetime(time.Minute * 5)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxIdleTime(time.Minute * 3)

	switch action {
	case "export":
		log.Println("Starting dump database")
		rows, err := db.Query("show tables")
		if err != nil {
			log.Fatal(err)
		}

		var tableList []string
		for rows.Next() {
			var tableName string

			err := rows.Scan(&tableName)
			if err != nil {
				log.Fatal(err)
			}
			tableList = append(tableList, tableName)
		}

		rows.Close()
		db.Close()

		log.Println(tableList)

		startDump := time.Now()
		concurrentDumps := make(chan struct{}, numberChilds)
		var wg sync.WaitGroup

		for _, table := range tableList {
			log.Println("Prepare dump for table " + table)
			wg.Add(1)
			go func(t string) {
				defer wg.Done()
				concurrentDumps <- struct{}{}
				dump_table(targetDir, serverName, userName, passWord, dataBase, t)
				<-concurrentDumps
			}(table)
		}

		wg.Wait()
		elapsedDump := time.Since(startDump)
		log.Println("Dump done successfully in " + elapsedDump.String())
	case "import":
		log.Println("Starting import database")
		_, err := db.Query("show databases like '" + dataBase + "'")
		if err != nil {
			if strings.Contains(err.Error(), "Unknown database") {
				log.Fatal("Database " + dataBase + " doesn't exists")
			} else {
				log.Fatal(err)
			}
		}

		dirList, _ := os.ReadDir(targetDir)
		log.Println(dirList)

		startImport := time.Now()
		concurrentImports := make(chan struct{}, numberChilds)
		var wg sync.WaitGroup

		for _, dir := range dirList {
			log.Println("Prepare import of file " + targetDir + "/" + dir.Name())
			wg.Add(1)
			go func(f string) {
				defer wg.Done()
				concurrentImports <- struct{}{}
				import_table(f, serverName, userName, passWord, dataBase)
				<-concurrentImports
			}(targetDir + "/" + dir.Name())
		}

		wg.Wait()
		elapsedImport := time.Since(startImport)
		log.Println("Import done successfully in " + elapsedImport.String())
	}
}
