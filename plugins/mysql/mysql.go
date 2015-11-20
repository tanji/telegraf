package mysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/influxdb/telegraf/plugins"
	"log"
	"strings"
	"time"
)

type Mysql struct {
	Servers        []string
	LastStatus     map[string]uint
	LastSampleTime time.Time
}

var sampleConfig = `
  # specify servers via a url matching:
  #  [username[:password]@][protocol[(address)]]/[?tls=[true|false|skip-verify]]
  #  see https://github.com/go-sql-driver/mysql#dsn-data-source-name
  #  e.g.
  #    root:passwd@tcp(127.0.0.1:3306)/?tls=false
  #    root@tcp(127.0.0.1:3306)/?tls=false
  #
  # If no servers are specified, then localhost is used as the host.
  servers = ["tcp(127.0.0.1:3306)/"]
`

func (m *Mysql) SampleConfig() string {
	return sampleConfig
}

func (m *Mysql) Description() string {
	return "Read metrics from one or many mysql servers"
}

var localhost = ""

func (m *Mysql) Gather(acc plugins.Accumulator) error {
	if len(m.Servers) == 0 {
		// if we can't get stats in this case, thats fine, don't report
		// an error.
		m.gatherServer(localhost, acc)
		return nil
	}

	for _, serv := range m.Servers {
		err := m.gatherServer(serv, acc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Mysql) gatherServer(serv string, acc plugins.Accumulator) error {
	// If user forgot the '/', add it
	if strings.HasSuffix(serv, ")") {
		serv = serv + "/"
	} else if serv == "localhost" {
		serv = ""
	}

	db, err := sql.Open("mysql", serv)
	if err != nil {
		return err
	}

	statusVars := make(map[string]uint)
	sampleTime := time.Now()

	defer func() {
		m.LastStatus = statusVars
		m.LastSampleTime = sampleTime
		db.Close()
	}()

	rows, err := db.Query("SELECT LOWER(VARIABLE_NAME) AS name, VARIABLE_VALUE AS val FROM INFORMATION_SCHEMA.GLOBAL_STATUS")
	if err != nil {
		return err
	}

	var servtag string
	servtag, err = parseDSN(serv)
	if err != nil {
		servtag = "localhost"
	}
	tags := map[string]string{"server": servtag}

	for rows.Next() {
		var name string
		var val uint

		err = rows.Scan(&name, &val)
		if err != nil {
			log.Printf("Could not parse variable %s as integer\n", name)
		}

		statusVars[name] = val
	}

	if m.LastStatus != nil && statusVars != nil {
		sampleDelta := sampleTime.Sub(m.LastSampleTime).Seconds()
		for k, v := range statusVars {
			for _, e := range MysqlCounterKeys {
				if e == k {
					i := diff(statusVars[k], m.LastStatus[k], uint(sampleDelta))
					acc.Add(k, i, tags)
				}
			}
			for _, e := range MysqlGaugeKeys {
				if e == k {
					acc.Add(k, v, tags)
				}
			}
		}
	}

	// Formula computation goes here

	maxConn, err := getVariableByName(db, "max_connections")
	if err != nil {
		return err
	}

	acc.Add("connections_max_reached_ratio", statusVars["max_used_connections"]/maxConn, tags)

	acc.Add("connections_refused_ratio", statusVars["aborted_connects"]/maxConn, tags)

	acc.Add("connection_usage", statusVars["threads_connected"]/maxConn, tags)

	acc.Add("innodb_buffer_pool_hit_ratio", statusVars["innodb_buffer_pool_reads"]/statusVars["innodb_buffer_pool_read_requests"], tags)

	acc.Add("thread_cache_hit_ratio", statusVars["threads_created"]/statusVars["connections"])

	acc.Add("table_lock_contention", statusVars["table_locks_waited"]/(statusVars["table_locks_waited"]+statusVars["table_locks_immediate"]), tags)

	acc.Add("qcache_hit_ratio", statusVars["qcache_hits"]/(statusVars["com_select"]+statusVars["qcache_hits"]), tags)

	acc.Add("table_cache_hit_ratio", statusVars["created_tmp_disk_tables"]/statusVars["created_tmp_tables"], tags)

	conn_rows, err := db.Query("SELECT user, sum(1) FROM INFORMATION_SCHEMA.PROCESSLIST GROUP BY user")

	for conn_rows.Next() {
		var user string
		var connections int64

		err = conn_rows.Scan(&user, &connections)
		if err != nil {
			return err
		}

		tags := map[string]string{"server": servtag, "user": user}

		if err != nil {
			return err
		}
		acc.Add("connections_per_user", connections, tags)
	}

	return nil
}

func init() {
	plugins.Add("mysql", func() plugins.Plugin {
		return &Mysql{}
	})
}

func diff(newVal, oldVal, sampleTime uint) uint {
	d := newVal - oldVal
	if d < 0 {
		d = newVal
	}
	return d / sampleTime
}

func getVariableByName(db *sql.DB, name string) (interface{}, error) {
	var val string
	row := db.QueryRow("SELECT variable_value FROM information_schema.global_status WHERE variable_name = $1", name)
	err := row.Scan(&val)
	if err != nil {
		return err
	}
	intval, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return val
	} else {
		return intval
	}
}
