package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"github.com/sfreiberg/simplessh"
	"flag"
	"os"
	"sync"
	"fmt"
)

func main() {
	// where data will be copied to
	dst_host := flag.String("dst_host", "", "-dst_host <STRING>")
	dst_db := flag.String("dst_db", "", "-dst_db <STRING>")

	// where data comes from
	src_db := flag.String("src_db", "", "-src_db <STRING>")

	// scp info
	id_file := flag.String("i", "/var/lib/mysql/.ssh/id_rsa", "-i <RSA_FILE>")
	scp_user := flag.String("u", "mysql", "-u <STRING>")

	src_dir := flag.String("src_base_dir", "/var/lib/mysql/", "-src_base_dir")
	dst_dir := flag.String("dst_base_dir", "/var/lib/mysql/", "-dst_base_dir")

	common_mysql_admin_user := flag.String("user", "mon", "-user <STRING>")
	common_mysql_admin_pass := flag.String("password", "", "-password <PASSWORD>")

	//
	// parse any command line arguments
	//
	flag.Parse()
	if len(*src_dir) < 1 {
		flag.PrintDefaults()
		return;
	}

	if len(*dst_host) < 1 {
		flag.PrintDefaults()
		return;
	}

	if (len(*src_db) < 1) {
		flag.PrintDefaults()
		return;
	}

	if (len(*dst_db) == 0) {
		*dst_db = *src_db
	}
	if (len(*dst_dir) == 0) {
		*dst_dir = *src_dir
	}

	*src_dir += *src_db
	*dst_dir += *dst_db

	_, err_dir := os.Stat(*src_dir)
	if err_dir != nil {
		panic(err_dir)
		return;
	}

	dst_connect_dsn := *common_mysql_admin_user + ":" + *common_mysql_admin_pass + "@tcp(" + *dst_host + ":3306)/" + *dst_db;
	src_connect_dsn := *common_mysql_admin_user + ":" + *common_mysql_admin_pass + "@tcp(127.0.0.1:3306)/" + *src_db;

	src, err_src := sql.Open("mysql", src_connect_dsn);
	dst, err_dst := sql.Open("mysql", dst_connect_dsn);


	tasks := make(chan string, 1024)

	if err_src != nil {
		panic(err_src)
		return;
	}

	if err_dst != nil {
		panic(err_dst)
		return;
	}

	defer src.Close()
	defer dst.Close()

	//
	// make sure both src and dst can wait for long periods
	//
	_, err_session := dst.Exec("SET SESSION WAIT_TIMEOUT=999999999")
	if err_session != nil {
		panic(err_session)
		return;
	}

	_, err_sess := src.Exec("SET SESSION WAIT_TIMEOUT=999999999")
	if err_sess != nil {
		panic(err_sess)
		return;
	}



	// get the list of tables on the source
	rows, err := src.Query("SHOW TABLES")
	if err != nil {
		panic(err)
	}

	var tables []string

	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			panic(err)
		}

		tables = append(tables, table)
	}

	if len(tables) == 0 {
		panic("There are no tables")
	}

	//size := len(tables)+1
	tready := make(chan string, 1024)

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	// create the list of tables ob the dst
	for _, tname := range tables {

		var table string;
		var tablestruct string;
		query := "SHOW CREATE TABLE " + tname
		err := src.QueryRow(query).Scan(&table, &tablestruct);
		if err != nil {
			panic(err)
			return;
		}

		_, err_drop := dst.Exec("DROP TABLE IF EXISTS " + table);
		if err_drop != nil {
			panic(err)
			return;
		}

		_, err_create := dst.Exec(tablestruct);
		if err_create != nil {
			panic(err)
			return;
		}

		// discard the tablespaces on the dst
		_, err_discard := dst.Exec("ALTER TABLE " + table + " DISCARD TABLESPACE")
		if err_discard != nil {
			panic(err)
			return;
		}

	}

	//
	// now on the src lock tables for export
	//
	tbl_str := strings.Join(tables, ",")

	_, err_flush := src.Exec("FLUSH TABLES " + tbl_str + " FOR EXPORT")
	if err_flush != nil {

		panic(err_flush)
	}

	// scp the tablespace from the src to the dst [pool]
	for i := 0; i < 4; i++ {
		go worker(i, tasks, tready, *src_dir, *dst_dir, *scp_user, *id_file, *dst_host)
	}

	for _, t := range tables {
		tasks <- t
	}

	close(tasks) // tell the channel there are no more tasks


	// import the tablespace on the dst
	var wg sync.WaitGroup
	for j := 1; j <= len(tables); j++ {
		//var timport string
		timport := <-tready
		wg.Add(1)
		go func() {
			defer wg.Done();

			query := "ALTER TABLE " + timport + " IMPORT TABLESPACE";
			if _, err := dst.Exec(query); err != nil {
				fmt.Errorf(err.Error())
				return;
			}

		}()
	}

	wg.Wait()
	close(tready)

	// unlock the src
	src.Exec("UNLOCK TABLES");

	fmt.Printf("%d tables copied", len(tables))

}

//
// worker that copies the data
//
func worker(id int, jobs <-chan string, results chan <- string, src_dir string, dst_dir string, scp_user string, id_file string, dst_host string) {

	for t := range jobs {
		src_path_cfg := src_dir + "/" + t + ".cfg"
		dst_path_cfg := dst_dir + "/" + t + ".cfg"

		src_path_ibd := src_dir + "/" + t + ".ibd"
		dst_path_ibd := dst_dir + "/" + t + ".ibd"

		client, err := simplessh.ConnectWithKeyFile(dst_host + ":22", scp_user, id_file)
		if err != nil {
			panic(err)
		}

		defer client.Close()

		err1 := client.Upload(src_path_cfg, dst_path_cfg)
		if err1 != nil {
			panic(err1)
		}

		err2 := client.Upload(src_path_ibd, dst_path_ibd)
		if err2 != nil {
			panic(err2)
		}

		results <- t
	}
}