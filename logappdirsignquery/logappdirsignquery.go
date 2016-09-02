package mlogs

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	bf "../bloomfilter"
	logger "../logger"
	mytools "../tools"
)

var allversion = "1000000000"

type numset struct {
	connect_counts int64
	query_counts   int64

	user_counts  map[string]int64
	bfUserCounts int64

	ip_count  map[string]int64
	bfIpCount int64

	path_counts  map[string]int64
	bfPathCounts int64

	NotExit_counts int64
	Root_counts    int64
	Last_counts    int64
	NoData_counts  int64
	BigRoot_counts int64
}

type logset struct {
	m_record_lock sync.RWMutex

	// version_count : usage version
	version_count map[string]*numset

	// allversion_count
	allversion_count *numset

	bfUserFilter  *bf.BfFilter
	bfTUserFilter *bf.BfFilter

	bfIpFilter  *bf.BfFilter
	bfTIpFilter *bf.BfFilter

	bfPathFilter  *bf.BfFilter
	bfTPathFilter *bf.BfFilter

	date string
}

var AppDirSignQueryCount logset

func DonotAutoinit() {
	AppDirSignQueryCount.init()
	//AppDirSignQueryCount.readcachefile()
	//go AppDirSignQueryCount.Out()
}

func (v *numset) init() {
	v.user_counts = make(map[string]int64)
	v.ip_count = make(map[string]int64)
	v.path_counts = make(map[string]int64)
}

func (v *logset) init() {
	v.version_count = make(map[string]*numset)
	v.allversion_count = &numset{}
	v.allversion_count.init()
	v.date = mytools.GetDateString()

	if v.bfUserFilter == nil {
		v.bfUserFilter = bf.NewBloomFilter(1000000000)
	} else {
		v.bfUserFilter.Reinit()
	}

	if v.bfTUserFilter == nil {
		v.bfTUserFilter = bf.NewBloomFilter(1000000000)
	} else {
		v.bfTUserFilter.Reinit()
	}

	if v.bfIpFilter == nil {
		v.bfIpFilter = bf.NewBloomFilter(1000000000)
	} else {
		v.bfIpFilter.Reinit()
	}
	if v.bfTIpFilter == nil {
		v.bfTIpFilter = bf.NewBloomFilter(1000000000)
	} else {
		v.bfTIpFilter.Reinit()
	}

	if v.bfPathFilter == nil {
		v.bfPathFilter = bf.NewBloomFilter(1000000000)
	} else {
		v.bfPathFilter.Reinit()
	}
	if v.bfTPathFilter == nil {
		v.bfTPathFilter = bf.NewBloomFilter(1000000000)
	} else {
		v.bfTPathFilter.Reinit()
	}
}

func (v *logset) Record(str_log string) {
	// 0              1      2      3          4             5             6      7       8     9    10    11
	//timeString, remoteIP, uuid, channelId, cliVersion, languageCode, pathMd5, result, index, mask, mcc, testFlag

	vec := strings.Split(str_log, "\t")
	if len(vec) != 12 {
		fmt.Println("wrong log, len = ", len(vec), str_log)
		return
	}

	datestring := mytools.GetDateString()
	version := vec[4]
	result := vec[7]

	// before record lock ,calc hash
	repeatUser := v.bfUserFilter.BloomFilterCheckAndAdd(version + vec[2])
	trepeatUser := v.bfTUserFilter.BloomFilterCheckAndAdd(allversion + vec[2])

	repeatIp := v.bfIpFilter.BloomFilterCheckAndAdd(version + vec[1])
	trepeatIp := v.bfTIpFilter.BloomFilterCheckAndAdd(allversion + vec[1])

	repeatPath := v.bfPathFilter.BloomFilterCheckAndAdd(version + vec[6])
	trepeatPath := v.bfTPathFilter.BloomFilterCheckAndAdd(allversion + vec[6])

	v.m_record_lock.Lock() //lock
	// day changed
	if v.date != datestring {
		v.OutImmediately()
		v.init()
		repeatUser = false
		trepeatUser = false
		repeatIp = false
		trepeatIp = false
		repeatPath = false
		trepeatPath = false
	}

	tmp_count := v.version_count[version]
	total_count := v.allversion_count

	if tmp_count == nil {
		v.version_count[version] = &numset{}
		v.version_count[version].init()
		tmp_count = v.version_count[version]
	}

	// index == 0
	if vec[8] == "0" {
		tmp_count.connect_counts++
		total_count.connect_counts++
	}
	tmp_count.query_counts++
	total_count.query_counts++

	if !repeatUser {
		tmp_count.bfUserCounts++
		//tmp_count.user_counts[vec[2]]++
	}
	if !trepeatUser {
		total_count.bfUserCounts++
		//total_count.user_counts[vec[2]]++
	}

	if !repeatIp {
		tmp_count.bfIpCount++
		//tmp_count.ip_count[vec[1]]++
	}
	if !trepeatIp {
		total_count.bfIpCount++
		//total_count.ip_count[vec[1]]++
	}

	if !repeatPath {
		tmp_count.bfPathCounts++
		//tmp_count.path_counts[vec[6]]++
	}
	if !trepeatPath {
		total_count.bfPathCounts++
		//total_count.path_counts[vec[6]]++
	}

	if result == "1" {
		tmp_count.NotExit_counts++
		total_count.NotExit_counts++
	} else if result == "2" {
		tmp_count.Last_counts++
		total_count.Last_counts++
	} else if result == "3" {
		tmp_count.Root_counts++
		total_count.Root_counts++
	} else if result == "4" {
		tmp_count.NoData_counts++
		total_count.NoData_counts++
	} else if result == "5" {
		tmp_count.BigRoot_counts++
		total_count.BigRoot_counts++
	}

	v.m_record_lock.Unlock() //unlock
}

// Write to file
func (v *logset) Out() {
	for {
		time.Sleep(3 * time.Second)
		v.m_record_lock.RLock()
		v.OutImmediately()
		v.m_record_lock.RUnlock()
		v.Release()
	}
}

func (v *logset) readcachefile() {
	midlog, err := logger.NewCacheLog("./", "appdirsignquerymiddle"+strings.Replace(v.date, "/", "-", -1)+".done")
	if err != nil {
		fmt.Fprintln(os.Stderr, time.Now(), "An error occurred on opening file : ", err)
		return
	}
	defer midlog.Close()

	for {
		readString, err := midlog.ReadLine()
		if err != nil {
			fmt.Fprintln(os.Stderr, time.Now(), err)
			break
		}
		if len(readString) == 0 {
			continue
		}

		v.loadData(readString)
	}

}

func (v *logset) loadData(readString string) {
	vec := strings.Split(readString, "\t")

	var i64 int64
	var err error

	if len(vec) == 3 {
		i64, err = strconv.ParseInt(vec[2], 10, 64)
		if err != nil {
			fmt.Fprintln(os.Stderr, time.Now(), err)
			return
		}
	} else if len(vec) == 4 {
		i64, err = strconv.ParseInt(vec[3], 10, 64)
		if err != nil {
			fmt.Fprintln(os.Stderr, time.Now(), err)
			return
		}
	}

	version := vec[0]
	tmp_count := v.version_count[version]
	if tmp_count == nil {
		v.version_count[version] = &numset{}
		v.version_count[version].init()
		tmp_count = v.version_count[version]
	}
	totalCount := v.allversion_count
	sign := vec[1]

	switch len(vec) {
	case 3:
		if sign == "connect_counts" {
			totalCount.connect_counts += i64
			tmp_count.connect_counts = i64
		} else if sign == "query_counts" {
			totalCount.query_counts += i64
			tmp_count.query_counts = i64
		} else if sign == "NotExit_counts" {
			totalCount.NotExit_counts += i64
			tmp_count.NotExit_counts = i64
		} else if sign == "Root_counts" {
			totalCount.Root_counts += i64
			tmp_count.Root_counts = i64
		} else if sign == "Last_counts" {
			totalCount.Last_counts += i64
			tmp_count.Last_counts = i64
		} else if sign == "NoData_counts" {
			totalCount.NoData_counts += i64
			tmp_count.NoData_counts = i64
		} else if sign == "BigRoot_counts" {
			totalCount.BigRoot_counts += i64
			tmp_count.BigRoot_counts = i64
		}
	case 4:
		if sign == "uuid" {
			totalCount.user_counts[vec[2]] += i64
			tmp_count.user_counts[vec[2]] = i64
		} else if sign == "ip" {
			totalCount.ip_count[vec[2]] += i64
			tmp_count.ip_count[vec[2]] = i64
		} else if sign == "path" {
			totalCount.path_counts[vec[2]] += i64
			tmp_count.path_counts[vec[2]] = i64
		}
	default:
		fmt.Fprintln(os.Stderr, time.Now(), "wrong data")
	}
}

func (v *logset) cacheinfile() {
	midlog, err := logger.NewDataLogger("./", "appdirsignquerymiddle"+strings.Replace(v.date, "/", "-", -1))
	if err != nil {
		fmt.Fprintln(os.Stderr, time.Now(), "An error occurred on opening file : ", err)
		panic(err)
	}
	defer midlog.Finish()

	for version, count := range v.version_count {
		midlog.Writeln(version, "\tconnect_counts\t", count.connect_counts)
		midlog.Writeln(version, "\tquery_counts\t", count.query_counts)
		midlog.Writeln(version, "\tNotExit_counts\t", count.NotExit_counts)
		midlog.Writeln(version, "\tRoot_counts\t", count.Root_counts)
		midlog.Writeln(version, "\tLast_counts\t", count.Last_counts)
		midlog.Writeln(version, "\tNoData_counts\t", count.NoData_counts)
		midlog.Writeln(version, "\tBigRoot_counts\t", count.BigRoot_counts)

		for uuid, num := range count.user_counts {
			midlog.Writeln(version, "\tuuid\t", uuid+"\t", num)
		}

		for remoteIP, num := range count.ip_count {
			midlog.Writeln(version, "\tip\t", remoteIP+"\t", num)
		}

		for path, num := range count.path_counts {
			midlog.Writeln(version, "\tpath\t", path+"\t", num)
		}
	}
}
func (v *logset) Release() {
	releasename := "./stat/appdirsignquery/" + strings.Replace(v.date, "-", "", -1) + "/dir_sign_query_stat_log.log"

	os.Rename("appdirsignquery"+strings.Replace(v.date, "/", "-", -1)+".done", releasename)

	cmd := exec.Command("gzip", "-S .gz.done", releasename)
	err := cmd.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, time.Now(), "gzip : ", err)
	}
}

func (v *logset) OutImmediately() {
	//v.cacheinfile()

	tmplog, err := logger.NewDataLogger("./", "appdirsignquery"+strings.Replace(v.date, "/", "-", -1))
	if err != nil {
		fmt.Fprintln(os.Stderr, time.Now(), "An error occurred on opening file : ", err)
		panic(err)
	}
	defer tmplog.Finish()

	//connect_counts	query_counts	user_counts	ip_count	path_counts	date	version	NotExit_counts	Root_counts	Last_counts	NoData_counts	BigRoot_counts

	// dump data to file
	tmplog.Writeln("connect_counts\tquery_counts\tuser_counts\tip_count\tpath_counts\tdate\tversion\tNotExit_counts\tRoot_counts\tLast_counts\tNoData_counts\tBigRoot_counts")

	total_count := v.allversion_count
	tmplog.Writeln(total_count.connect_counts, "\t",
		total_count.query_counts, "\t",
		//len(total_count.user_counts), "\t",
		total_count.bfUserCounts, "\t",
		//len(total_count.ip_count), "\t",
		total_count.bfIpCount, "\t",
		//len(total_count.path_counts), "\t",
		total_count.bfPathCounts, "\t",
		v.date, "\t",
		1000000000, "\t",
		total_count.NotExit_counts, "\t",
		total_count.Root_counts, "\t",
		total_count.Last_counts, "\t",
		total_count.NoData_counts, "\t",
		total_count.BigRoot_counts)

	for version, count := range v.version_count {
		tmplog.Writeln(count.connect_counts, "\t",
			count.query_counts, "\t",
			//len(count.user_counts), "\t",
			count.bfUserCounts, "\t",
			//len(count.ip_count), "\t",
			count.bfIpCount, "\t",
			//len(count.path_counts), "\t",
			count.bfPathCounts, "\t",
			v.date, "\t",
			version, "\t",
			count.NotExit_counts, "\t",
			count.Root_counts, "\t",
			count.Last_counts, "\t",
			count.NoData_counts, "\t",
			count.BigRoot_counts)
	}
}
