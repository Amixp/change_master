package main

import (
	"code.google.com/p/gcfg"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Default struct {
		Method       string
		User         string
		Password     string
		ReplUser     string
		ReplPassword string
		Port         int
	}

	Master struct {
		Host     string
		User     string
		Password string
		Port     int
	}

	Slave map[string]*struct {
		Host         string
		User         string
		Password     string
		Port         int
		ReplUser     string
		ReplPassword string
	}
}

type SlaveStatus struct {
	Slave_IO_State                string
	Master_Host                   string
	Master_User                   string
	Master_Port                   string
	Connect_Retry                 string
	Master_Log_File               string
	Read_Master_Log_Pos           int64
	Relay_Log_File                string
	Relay_Log_Pos                 int64
	Relay_Master_Log_File         string
	Slave_IO_Running              string
	Slave_SQL_Running             string
	Replicate_Do_DB               string
	Replicate_Ignore_DB           string
	Replicate_Do_Table            string
	Replicate_Ignore_Table        string
	Replicate_Wild_Do_Table       string
	Replicate_Wild_Ignore_Table   string
	Last_Errno                    string
	Last_Error                    string
	Skip_Counter                  string
	Exec_Master_Log_Pos           int64
	Relay_Log_Space               int64
	Until_Condition               string
	Until_Log_File                string
	Until_Log_Pos                 string
	Master_SSL_Allowed            string
	Master_SSL_CA_File            string
	Master_SSL_CA_Path            string
	Master_SSL_Cert               string
	Master_SSL_Cipher             string
	Master_SSL_Key                string
	Seconds_Behind_Master         sql.NullInt64
	Master_SSL_Verify_Server_Cert string
	Last_IO_Errno                 string
	Last_IO_Error                 string
	Last_SQL_Errno                string
	Last_SQL_Error                string
	Replicate_Ignore_Server_Ids   string
	Master_Server_Id              string
}

type MasterStatus struct {
	File             string
	Position         int64
	Binlog_Do_DB     string
	Binlog_Ignore_DB string
}

var cfg Config
var Methods = []string{"up", "down", "middle"}
var old_master string

func Usage() {
	fmt.Println("Usage:")
	fmt.Println("   ./change_master [config.cfg]")
	os.Exit(1)
}

func ConnectMySQL(dataSourceName string) (*sql.DB, error) {
	conn, err := sql.Open("mysql", dataSourceName)
	conn.SetMaxOpenConns(1)
	return conn, err
}

// ShowSlaveStatus
// @input
// @output

func ShowSlaveStatus(conn *sql.DB) (SlaveStatus, error) {
	sql := "show slave status"
	var s SlaveStatus
	err := conn.QueryRow(sql).Scan(&s.Slave_IO_State, &s.Master_Host, &s.Master_User, &s.Master_Port, &s.Connect_Retry,
		&s.Master_Log_File, &s.Read_Master_Log_Pos, &s.Relay_Log_File, &s.Relay_Log_Pos,
		&s.Relay_Master_Log_File, &s.Slave_IO_Running, &s.Slave_SQL_Running, &s.Replicate_Do_DB,
		&s.Replicate_Ignore_DB, &s.Replicate_Do_Table, &s.Replicate_Ignore_Table,
		&s.Replicate_Wild_Do_Table, &s.Replicate_Wild_Ignore_Table, &s.Last_Errno,
		&s.Last_Error, &s.Skip_Counter, &s.Exec_Master_Log_Pos, &s.Relay_Log_Space,
		&s.Until_Condition, &s.Until_Log_File, &s.Until_Log_Pos, &s.Master_SSL_Allowed,
		&s.Master_SSL_CA_File, &s.Master_SSL_CA_Path, &s.Master_SSL_Cert, &s.Master_SSL_Cipher,
		&s.Master_SSL_Key, &s.Seconds_Behind_Master, &s.Master_SSL_Verify_Server_Cert,
		&s.Last_IO_Errno, &s.Last_IO_Error, &s.Last_SQL_Errno, &s.Last_SQL_Error,
		&s.Replicate_Ignore_Server_Ids, &s.Master_Server_Id)
	return s, err
}

// StopSlave
// @input
// @output
func StopSlave(conn *sql.DB, thread string) bool {
	var sql string
	var s SlaveStatus

	if thread == "io" {
		sql = "stop slave io_thread"
	} else {
		v, _ := SlaveTempTableStatus(conn)
		if v > 0 {
			log.Fatalln("slave has opening temporary table, can't stop")
		} else {
			if thread == "sql" {
				sql = "stop slave sql_thread"
			} else {
				sql = "stop slave"
			}
		}
	}
	if sql != "" {
		conn.Exec(sql)
	} else {
		return false
	}

	// check stop slave status
	for i := 0; i < 4; i++ {
		s, _ = ShowSlaveStatus(conn)
		if thread == "io" && s.Slave_IO_Running != "No" {
			if i != 3 {
				log.Println("wait 1 second for stop slave")
				time.Sleep(time.Second)
			} else {
				log.Fatalln("Stop slave_io_thread Failed!")
			}
		} else if thread == "sql" && s.Slave_SQL_Running != "No" {
			if i != 3 {
				log.Println("wait 1 second for stop slave")
				time.Sleep(time.Second)
			} else {
				log.Fatalln("Stop slave_sql_thread Failed!")
			}
		} else if thread == "" && thread == "io" && s.Slave_IO_Running != "No" && s.Slave_SQL_Running != "No" {
			if i != 3 {
				log.Println("wait 1 second for stop slave")
				time.Sleep(time.Second)
			} else {
				log.Fatalln("Stop slave Failed!\nSlave_IO_Running : " + s.Slave_IO_Running + "\nSlave_SQL_Running: " + s.Slave_SQL_Running)
			}
		}
	}
	return true
}

func StartSlave(conn *sql.DB, thread string) bool {
	sql := "start slave"
	if thread == "io" {
		sql = "stop slave io_thread"
	}
	if thread == "sql" {
		sql = "stop slave sql_thread"
	}

	var s SlaveStatus
	_, err := conn.Exec(sql)
	if err != nil {
		log.Fatalln("Start slave Failed!")
	}

	// check start slave status
	for i := 0; i < 4; i++ {
		s, _ = ShowSlaveStatus(conn)
		if i == 3 && (s.Slave_IO_Running != "Yes" || s.Slave_SQL_Running != "Yes") {
			log.Println("Start slave Failed!\nSlave_IO_Running : " + s.Slave_IO_Running + "\nSlave_SQL_Running: " + s.Slave_SQL_Running)
		} else if s.Slave_IO_Running == "Yes" && s.Slave_SQL_Running == "Yes" {
			break
		} else {
			log.Println("wait 1 second for start slave check")
			time.Sleep(time.Second)
		}
	}
	return true
}

func StartSlaveUntil(conn *sql.DB, log_file string, log_pos string) error {
	sql := "select @@hostname"
	var hostname string
	conn.QueryRow(sql).Scan(&hostname)

	sql = "start slave until master_log_file='" + log_file + "', master_log_pos=" + log_pos
	var s SlaveStatus
	_, err := conn.Exec(sql)
	log.Println("slave", hostname, sql)
	if err != nil {
		log.Fatalln("Start slave Failed!")
	}

	// check start slave status
	for i := 0; i < 4; i++ {
		s, _ = ShowSlaveStatus(conn)
		if (s.Slave_IO_Running != "Yes" || s.Slave_SQL_Running != "Yes") && (strconv.FormatInt(s.Exec_Master_Log_Pos, 10) == log_pos || s.Relay_Master_Log_File == log_file) {
			log.Println("slave", hostname+" start slave until catched master")
			break
		} else {
			if i != 3 {
				log.Println("slave", hostname+" wait 1 second for start slave until")
				time.Sleep(time.Second)
			} else {
				log.Fatalln("Start slave until Failed!\nSlave_IO_Running : " + s.Slave_IO_Running + "\nSlave_SQL_Running: " + s.Slave_SQL_Running + "\nRelay_Master_Log_File: " + s.Slave_SQL_Running + "\nExec_Master_Log_Pos: " + s.Slave_SQL_Running)
			}
		}
	}
	return err
}

//
//
//
func ChangeMaster(conn *sql.DB, master_host string, master_user string, master_password string,
	master_port string, master_log_file string, master_log_pos string) bool {
	sql := "select @@hostname"
	var hostname string
	conn.QueryRow(sql).Scan(&hostname)
	log.Println("slave", hostname, "begin change master")

	sql = "CHANGE MASTER TO MASTER_HOST='" + master_host + "',MASTER_USER='" + master_user + "',MASTER_PASSWORD='" + master_password +
		"', MASTER_PORT=" + master_port + ", MASTER_LOG_FILE='" + master_log_file + "',MASTER_LOG_POS=" + master_log_pos
	log.Println(sql)
	_, err := conn.Exec(sql)
	if err != nil {
		log.Fatalln("Change master Failed!")
	}
	return StartSlave(conn, "")
}

// ShowMasterStatus
// @input
// @output
func ShowMasterStatus(conn *sql.DB) (MasterStatus, error) {
	sql := "show master status"
	var m MasterStatus
	err := conn.QueryRow(sql).Scan(&m.File, &m.Position, &m.Binlog_Do_DB, &m.Binlog_Ignore_DB)
	return m, err
}

// @input conn *sql.DB
func SlaveTempTableStatus(conn *sql.DB) (int, error) {
	sql := "show global status like 'Slave_open_temp_tables'"
	var Variable_name string
	var Value int
	err := conn.QueryRow(sql).Scan(&Variable_name, &Value)
	return Value, err
}

//
//
//
func CheckGrants(conn *sql.DB) error {

	var HaveReplClient string // show master status, show slave status
	var HaveSuper string      // stop slave, start slave, change mater to

	sql := "select current_user"
	var current_user string
	err := conn.QueryRow(sql).Scan(&current_user)
	current_user = "'" + strings.Replace(current_user, "@", "'@'", 1) + "'"
	sql = "select PRIVILEGE_TYPE from information_schema.USER_PRIVILEGES where GRANTEE=\"" + current_user + "\""
	rows, err := conn.Query(sql)
	if err != nil {
		log.Fatalln(sql, err)
	}
	var privilege string
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&privilege)
		if privilege == "REPLICATION CLIENT" {
			HaveReplClient = "Yes"
		}
		if privilege == "SUPER" {
			HaveSuper = "Yes"
		}
	}

	if HaveSuper == "" {
		log.Fatalln("Don't have super privilege")
	}

	if HaveReplClient == "" {
		log.Fatalln("Don't have replication client privilege")
	}
	return err
}

//
//
//
func CheckReplGrant(conn *sql.DB) {
	var host string
	var HaveReplSlave string
	for s := range cfg.Slave {
		sql := "SELECT host from mysql.user where user=\"" + cfg.Default.ReplUser + "\""
		rows, _ := conn.Query(sql)
		defer rows.Close()
		for rows.Next() {
			rows.Scan(&host)
			pre := strings.Split(cfg.Slave[s].Host, ".")
			for i := 0; i < len(pre); i++ {
				p := strings.Join(pre[:i+1], ".") + ".%"
				if host == cfg.Slave[s].Host || host == p {
					HaveReplSlave = "Yes"
				}
			}
		}
		if HaveReplSlave != "Yes" {
			log.Fatalln(cfg.Slave[s].Host + " not have replication slave privilege")
		}
	}
}

//
//
//
func CheckVersion(conn *sql.DB, version string) (bool, error) {
	sql := "select version"
	var vers string
	err := conn.QueryRow(sql).Scan(&vers)
	if strings.Index(vers, version) == 0 {
		return true, err
	}
	return false, err
}

//
//
//
func InitConfigStruct() {
	// default section check & initial
	if cfg.Default.Method == "" {
		cfg.Default.Method = "middle"
		log.Fatalln("Not config change method use 'middle' by default")
	}
	if cfg.Default.Port == 0 {
		cfg.Default.Port = 3306
	}

	// master section check & initial
	if cfg.Master.Host == "" {
		log.Fatalln("Not give new master host in master section")
	}
	if cfg.Master.User == "" {
		if cfg.Default.User == "" {
			log.Fatalln("Not give admin user config in default section")
		}
		cfg.Master.User = cfg.Default.User
	}
	if cfg.Master.Password == "" {
		cfg.Master.Password = cfg.Default.Password
	}
	if cfg.Master.Port == 0 {
		cfg.Master.Port = cfg.Default.Port
	}

	// slave sub section check & initial
	for s := range cfg.Slave {
		if cfg.Slave[s].Host == "" {
			log.Fatalln("Not give new master host in slave section " + s)
		}
		if cfg.Slave[s].User == "" {
			if cfg.Default.User == "" {
				log.Fatalln("Not give admin user config in default section")
			}
			cfg.Slave[s].User = cfg.Default.User
		}
		if cfg.Slave[s].Password == "" {
			cfg.Slave[s].Password = cfg.Default.Password
		}
		if cfg.Slave[s].Port == 0 {
			cfg.Slave[s].Port = cfg.Default.Port
		}
	}

	for s := range cfg.Slave {
		for t := range cfg.Slave {
			if s != t && cfg.Slave[s].Host == cfg.Slave[t].Host && cfg.Slave[s].Port == cfg.Slave[t].Port {
				log.Fatalln("duplicate slave in configuration")
			}
		}
	}
}

// LoadConfig
// Load configration from config.cfg default
// @input nil
// @output nil
func LoadConfig() {
	var ConfigFile = ""
	if len(os.Args) > 1 {
		ConfigFile = os.Args[1]
	} else {
		ConfigFile = "./config.cfg"
	}

	if _, err := os.Stat(ConfigFile); err != nil {
		log.Fatalln("Config File Path not exist.")
	}

	err := gcfg.ReadFileInto(&cfg, ConfigFile)
	if err != nil {
		log.Fatalln("Config Format Error: ", err)
	}
	InitConfigStruct()
}

func MiddleChange(MasterConn *sql.DB, OldMasterConn *sql.DB, SlaveConn []*sql.DB) {
	ms, _ := ShowSlaveStatus(MasterConn)
	oms, _ := ShowSlaveStatus(OldMasterConn)
	if ms.Master_Host != oms.Master_Host {
		log.Fatalln("Two middle master not replicate from the same master")
	}

	// check middle master replication speed
	diff := (ms.Exec_Master_Log_Pos - oms.Exec_Master_Log_Pos)
	if diff > 10000 || diff < -10000 {
		log.Fatalln("Two middle master replicate speed far away different")
	}

	//
	log.Println("old master(" + old_master + ") stop slave io_thraed")
	log.Println("new master(" + cfg.Master.Host + ":" + strconv.Itoa(cfg.Master.Port) + ") stop slave io_thraed")
	StopSlave(MasterConn, "io")
	StopSlave(OldMasterConn, "io")

	// wait until sql thread catch io thread
	for i := 0; i < 4; i++ {
		ms, _ = ShowSlaveStatus(MasterConn)
		oms, _ = ShowSlaveStatus(OldMasterConn)

		if ms.Exec_Master_Log_Pos == ms.Read_Master_Log_Pos && oms.Exec_Master_Log_Pos == oms.Read_Master_Log_Pos {
			log.Println("for safe stop two master replication")
			StopSlave(MasterConn, "")
			StopSlave(OldMasterConn, "")
			log.Println("old master("+old_master+") slave status backup:", oms.Master_Host, oms.Master_User, oms.Master_Port, oms.Relay_Master_Log_File, oms.Exec_Master_Log_Pos)
			log.Println("new master("+cfg.Master.Host+":"+strconv.Itoa(cfg.Master.Port)+") slave status backup:", ms.Master_Host, ms.Master_User, ms.Master_Port, ms.Relay_Master_Log_File, ms.Exec_Master_Log_Pos)
			break
		} else {
			if i != 3 {
				log.Println("wait 1 second for master sql thread catch up with io thread")
				time.Sleep(time.Second)
			} else {
				StartSlave(MasterConn, "")
				StartSlave(OldMasterConn, "")
				log.Fatalln("SQL thread take too much time to catch io thread.\nAuto restart middle master replication & stop other operation")
			}
		}
	}

	// make two middle master stop at the same position
	var omms MasterStatus
	if ms.Exec_Master_Log_Pos == oms.Exec_Master_Log_Pos && ms.Relay_Master_Log_File == oms.Relay_Master_Log_File {
		omms, _ = ShowMasterStatus(OldMasterConn)
	} else {
		if ms.Exec_Master_Log_Pos > oms.Exec_Master_Log_Pos {
			StartSlaveUntil(OldMasterConn, ms.Relay_Master_Log_File, strconv.FormatInt(ms.Exec_Master_Log_Pos, 10))
			for i := 0; i < 4; i++ {
				oms, _ = ShowSlaveStatus(OldMasterConn)
				if ms.Exec_Master_Log_Pos == oms.Exec_Master_Log_Pos && ms.Relay_Master_Log_File == oms.Relay_Master_Log_File {
					omms, _ = ShowMasterStatus(OldMasterConn)
					break
				} else {
					if i != 3 {
						log.Println("wait 1 second for old master catch up with new master")
						time.Sleep(time.Second)
					} else {
						StartSlave(MasterConn, "")
						StartSlave(OldMasterConn, "")
						log.Fatalln("old master can't catch up with new master, start replication & stop other operations")
					}
				}
			}
		} else {
			StartSlaveUntil(MasterConn, oms.Relay_Master_Log_File, strconv.FormatInt(oms.Exec_Master_Log_Pos, 10))
			for i := 0; i < 4; i++ {
				ms, _ = ShowSlaveStatus(MasterConn)
				if ms.Exec_Master_Log_Pos == oms.Exec_Master_Log_Pos && ms.Relay_Master_Log_File == oms.Relay_Master_Log_File {
					omms, _ = ShowMasterStatus(OldMasterConn)
					break
				} else {
					if i != 3 {
						log.Println("wait 1 second for new master catch up with old master")
						time.Sleep(time.Second)
					} else {
						StartSlave(MasterConn, "")
						StartSlave(OldMasterConn, "")
						log.Fatalln("new master can't catch up with old master, start replication & stop other operations")
					}
				}
			}
		}
	}

	log.Println("two master stop at the same position:", ms.Relay_Master_Log_File, ms.Exec_Master_Log_Pos)
	log.Println("old master("+old_master+") master status backup:", omms.File, omms.Position)
	mms, _ := ShowMasterStatus(MasterConn)
	log.Println("new master("+cfg.Master.Host+":"+strconv.Itoa(cfg.Master.Port)+") master status backup:", mms.File, mms.Position)

	var ss SlaveStatus
	// check all slave has catch middle master
	for s := range SlaveConn {
		sql := "select @@hostname"
		var hostname string
		SlaveConn[s].QueryRow(sql).Scan(&hostname)
		for i := 0; i < 4; i++ {
			ss, _ = ShowSlaveStatus(SlaveConn[s])
			if ss.Exec_Master_Log_Pos == omms.Position && ss.Relay_Master_Log_File == omms.File {
				log.Println("slave", hostname+" slave status backup:", ss.Master_Host, ss.Master_User, ss.Master_Port, ss.Relay_Master_Log_File, ss.Exec_Master_Log_Pos)
				// stop slave && change master
				StopSlave(SlaveConn[s], "")
				ChangeMaster(SlaveConn[s], cfg.Master.Host, cfg.Default.ReplUser, cfg.Default.ReplPassword, strconv.Itoa(cfg.Master.Port),
					mms.File, strconv.FormatInt(mms.Position, 10))
				log.Println("slave", hostname, "change master done")
				break
			} else {
				if i != 3 {
					log.Println("wait 1 second for slave catch up with master")
					time.Sleep(time.Second)
				} else {
					log.Println("Slave can't catch middle master quickly, jump this slave")
				}
			}
		}
	}

	log.Println("old master(" + old_master + ") start slave")
	log.Println("new master(" + cfg.Master.Host + ":" + strconv.Itoa(cfg.Master.Port) + ") start slave")
	StartSlave(MasterConn, "")
	StartSlave(OldMasterConn, "")
	log.Println("Middle master change finished")
}

func UpChange(MasterConn *sql.DB, OldMasterConn *sql.DB, SlaveConn []*sql.DB) {
	var omms MasterStatus
	oms, _ := ShowSlaveStatus(OldMasterConn)

	if oms.Master_Host != cfg.Master.Host {
		log.Fatalln("old master not slave of new master")
	}

	log.Println("old master(" + old_master + ") stop slave io thread")
	StopSlave(OldMasterConn, "io")

	for i := 0; i < 4; i++ {
		if oms.Exec_Master_Log_Pos == oms.Read_Master_Log_Pos {
			log.Println("old master(" + old_master + ")'s sql thread catched up which io thread'")
			log.Println("old master("+old_master+") slave status backup:", oms.Master_Host, oms.Master_User, oms.Master_Port, oms.Relay_Master_Log_File, oms.Exec_Master_Log_Pos)
			omms, _ = ShowMasterStatus(OldMasterConn)
			log.Println("old master(" + old_master + ") show master status:" + omms.File + " " + strconv.FormatInt(omms.Position, 10))
			break
		} else {
			if i != 3 {
				log.Println("wait 1 second for sql thread catch up io thread")
				time.Sleep(time.Second)
			} else {
				StartSlave(OldMasterConn, "")
				log.Fatalln("old master sql_thread can't catch io thread")
			}
		}
	}

	for s := range SlaveConn {
		sql := "select @@hostname"
		var hostname string
		SlaveConn[s].QueryRow(sql).Scan(&hostname)
		for i := 0; i < 4; i++ {
			ss, _ := ShowSlaveStatus(SlaveConn[s])
			if ss.Exec_Master_Log_Pos == omms.Position && ss.Relay_Master_Log_File == omms.File {
				log.Println("slave", hostname, "stop slave")
				log.Println("slave", hostname+" slave status backup:", ss.Master_Host, ss.Master_User, ss.Master_Port, ss.Relay_Master_Log_File, ss.Exec_Master_Log_Pos)
				StopSlave(SlaveConn[s], "")
				ChangeMaster(SlaveConn[s], cfg.Master.Host, cfg.Default.ReplUser, cfg.Default.ReplPassword, strconv.Itoa(cfg.Master.Port),
					oms.Relay_Master_Log_File, strconv.FormatInt(oms.Exec_Master_Log_Pos, 10))
				log.Println("slave", hostname, "change master done")
				break
			} else {
				if i != 3 {
					log.Println("wait 1 second for slave(" + hostname + ") catch up with old master")
					time.Sleep(time.Second)
				} else {
					StartSlave(OldMasterConn, "")
					log.Fatalln("Slave can't catch middle master.\nstart middle master replication & stop other operation")
					break
				}
			}
		}
	}

	StartSlave(OldMasterConn, "")
	log.Println("old master(" + old_master + ") start slave")
	log.Println("Upward change master success")
}

func DownChange(MasterConn *sql.DB, OldMasterConn *sql.DB, SlaveConn []*sql.DB) {
	var mms MasterStatus
	var ms SlaveStatus
	var ss SlaveStatus

	// check new master and slave have the same master
	ms, _ = ShowSlaveStatus(MasterConn)
	for s := range SlaveConn {
		ss, _ = ShowSlaveStatus(SlaveConn[s])
		if ms.Master_Host != ss.Master_Host {
			log.Fatalln("new master and slave have different master, can't change master directly")
		}
	}

	// stop all slave replication
	log.Println("stop all slave replication")
	for s := range SlaveConn {
		StopSlave(SlaveConn[s], "")
	}

	var fast_pos int64
	for s := range SlaveConn {
		sql := "select @@hostname"
		var hostname string
		SlaveConn[s].QueryRow(sql).Scan(&hostname)
		for i := 0; i < 4; i++ {
			ss, _ = ShowSlaveStatus(SlaveConn[s])
			if ss.Exec_Master_Log_Pos == ss.Read_Master_Log_Pos {
				log.Println("slave", hostname+" slave status backup:", ss.Master_Host, ss.Master_User, ss.Master_Port, ss.Relay_Master_Log_File, ss.Exec_Master_Log_Pos)
				if ss.Exec_Master_Log_Pos > fast_pos {
					fast_pos = ss.Exec_Master_Log_Pos
				}
				break
			} else {
				if i == 3 {
					StartSlave(SlaveConn[s], "")
					log.Fatalln("Slave(" + hostname + ") can't catch up with new master\nrestart slave & stop other operation")
					break
				}
				log.Println(hostname + " wait 1 second for slave sql thread catch up with io thread")
				time.Sleep(time.Second)
			}
		}
	}

	for i := 0; i < 4; i++ {
		StopSlave(MasterConn, "")
		ms, _ = ShowSlaveStatus(MasterConn)
		if fast_pos > ms.Exec_Master_Log_Pos {
			StartSlave(MasterConn, "")
			if i != 3 {
				log.Println("wait 1 second")
				time.Sleep(time.Second)
			}
		} else {
			log.Println("new master("+cfg.Master.Host+":"+strconv.Itoa(cfg.Master.Port)+")slave status backup:", ms.Master_Host, ms.Master_User, ms.Master_Port, ms.Relay_Master_Log_File, ms.Exec_Master_Log_Pos)
			mms, _ = ShowMasterStatus(MasterConn)
			log.Println("new master("+cfg.Master.Host+":"+strconv.Itoa(cfg.Master.Port)+")show master status:", mms.File, mms.Position)
			break
		}
	}

	if fast_pos <= ms.Exec_Master_Log_Pos {
		for s := range SlaveConn {
			StartSlaveUntil(SlaveConn[s], ms.Master_Log_File, strconv.FormatInt(ms.Exec_Master_Log_Pos, 10))
			sql := "select @@hostname"
			var hostname string
			SlaveConn[s].QueryRow(sql).Scan(&hostname)
			for i := 0; i < 4; i++ {
				ss, _ := ShowSlaveStatus(SlaveConn[s])
				if ss.Exec_Master_Log_Pos == ms.Exec_Master_Log_Pos {
					StopSlave(SlaveConn[s], "")
					ChangeMaster(SlaveConn[s], cfg.Master.Host, cfg.Default.ReplUser, cfg.Default.ReplPassword, strconv.Itoa(cfg.Master.Port),
						mms.File, strconv.FormatInt(mms.Position, 10))
					log.Println("slave", hostname, "change master done")
					break
				} else {
					if i != 3 {
						log.Println("wait 1 second for slave catch up")
						time.Sleep(time.Second)
					} else {
						StartSlave(SlaveConn[s], "")
						log.Println("Slave can't catch up with new master")
					}
				}
			}
		}
	} else {
		for s := range SlaveConn {
			log.Println("master slow than slaves, not change start all slaves")
			StartSlave(SlaveConn[s], "")
		}
	}
	StartSlave(MasterConn, "")
	log.Println("new master(" + cfg.Master.Host + ":" + strconv.Itoa(cfg.Master.Port) + ") start slave done")
	log.Println("Downward change master finished")
}

func main() {
	LoadConfig()
	var MasterDSN, OldMasterDSN string
	var SlaveDSN []string
	var MasterConn, OldMasterConn *sql.DB
	var SlaveConn []*sql.DB
	MasterDSN = cfg.Master.User + ":" + cfg.Master.Password + "@tcp(" + cfg.Master.Host + ":" + strconv.Itoa(cfg.Master.Port) + ")/"
	for s := range cfg.Slave {
		dsn := cfg.Slave[s].User + ":" + cfg.Slave[s].Password + "@tcp(" + cfg.Slave[s].Host + ":" + strconv.Itoa(cfg.Slave[s].Port) + ")/"
		SlaveDSN = append(SlaveDSN, dsn)
	}

	// create connections
	MasterConn, _ = ConnectMySQL(MasterDSN)
	defer MasterConn.Close()
	for dsn := range SlaveDSN {
		conn, _ := ConnectMySQL(SlaveDSN[dsn])
		defer conn.Close()
		SlaveConn = append(SlaveConn, conn)
	}

	// check version && privilege && slave status
	CheckVersion(MasterConn, "5.5")
	CheckGrants(MasterConn)
	CheckReplGrant(MasterConn)

	for i := range SlaveConn {
		CheckVersion(SlaveConn[i], "5.5")
		CheckGrants(SlaveConn[i])
		ss, _ := ShowSlaveStatus(SlaveConn[i])
		if ss.Slave_IO_Running != "Yes" || ss.Slave_SQL_Running != "Yes" {
			log.Fatalln("Slave status error: " + SlaveDSN[i])
		}
		if old_master != "" && old_master != ss.Master_Host {
			log.Fatalln("Slaves have diffent master")
		} else {
			old_master = ss.Master_Host
		}

		if old_master == cfg.Master.Host && ss.Master_Port == strconv.Itoa(cfg.Master.Port) {
			log.Fatalln("old master(" + old_master + ") same with new master(" + cfg.Master.Host + ")")
		}
	}

	OldMasterDSN = cfg.Default.User + ":" + cfg.Default.Password + "@tcp(" + old_master + ":" + strconv.Itoa(cfg.Default.Port) + ")/"
	old_master = old_master + ":" + strconv.Itoa(cfg.Default.Port)
	OldMasterConn, _ = ConnectMySQL(OldMasterDSN)
	defer OldMasterConn.Close()
	CheckVersion(OldMasterConn, "5.5")
	CheckGrants(OldMasterConn)
	CheckReplGrant(OldMasterConn)

	switch {
	case cfg.Default.Method == "middle":
		MiddleChange(MasterConn, OldMasterConn, SlaveConn)
	case cfg.Default.Method == "up":
		UpChange(MasterConn, OldMasterConn, SlaveConn)
	case cfg.Default.Method == "down":
		DownChange(MasterConn, OldMasterConn, SlaveConn)
	default:
		MiddleChange(MasterConn, OldMasterConn, SlaveConn)
	}
}
