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
	user_counts  map[string]int64
	bfUserCounts int64

	ip_count  map[string]int64
	bfIpCount int64

	package_count map[string]int64
	bfPackCount   int64

	connect_counts int64
	query_counts   int64
	match_count    int64
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

	bfPackFilter  *bf.BfFilter
	bfTPackFilter *bf.BfFilter

	date string
}

var CachingpackagesignqueryCount logset

func DonotAutoinit() {
	CachingpackagesignqueryCount.init()
	//CachingpackagesignqueryCount.readcachefile()
	//go CachingpackagesignqueryCount.Out()
}

func (v *numset) init() {
	v.user_counts = make(map[string]int64)
	v.ip_count = make(map[string]int64)
	v.package_count = make(map[string]int64)
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

	if v.bfPackFilter == nil {
		v.bfPackFilter = bf.NewBloomFilter(1000000000)
	} else {
		v.bfPackFilter.Reinit()
	}
	if v.bfTPackFilter == nil {
		v.bfTPackFilter = bf.NewBloomFilter(1000000000)
	} else {
		v.bfTPackFilter.Reinit()
	}

}

func (v *logset) Record(str_log string) {
	// 0            1      2      3          4             5             6        7       8     9          10
	//timeStr, remoteIP, uuid, channelId, cliVersion, languageCode, packageMd5, signId, index, maskResult, mcc

	vec := strings.Split(str_log, "\t")
	if len(vec) != 11 {
		fmt.Println("wrong log, len = ", len(vec), str_log)
		return
	}

	datestring := mytools.GetDateString()
	version := vec[4]
	signId := vec[7]

	repeatUser := v.bfUserFilter.BloomFilterCheckAndAdd(version + vec[2])
	trepeatUser := v.bfTUserFilter.BloomFilterCheckAndAdd(allversion + vec[2])

	repeatIp := v.bfIpFilter.BloomFilterCheckAndAdd(version + vec[1])
	trepeatIp := v.bfTIpFilter.BloomFilterCheckAndAdd(allversion + vec[1])

	repeatPack := v.bfPackFilter.BloomFilterCheckAndAdd(version + vec[6])
	trepeatPack := v.bfTPackFilter.BloomFilterCheckAndAdd(allversion + vec[6])

	v.m_record_lock.Lock() //lock
	// day changed
	if v.date != datestring {
		v.outImmediately()
		v.init()
		repeatUser = false
		trepeatUser = false
		repeatIp = false
		trepeatIp = false
		repeatPack = false
		trepeatPack = false
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

	if signId != "0" {
		tmp_count.match_count++
		total_count.match_count++
	}

	//tmp_count.user_counts[vec[2]]++
	//total_count.user_counts[vec[2]]++
	if !repeatUser {
		tmp_count.bfUserCounts++
	}
	if !trepeatUser {
		total_count.bfUserCounts++
	}

	//tmp_count.ip_count[vec[1]]++
	//total_count.ip_count[vec[1]]++
	if !repeatIp {
		tmp_count.bfIpCount++
	}
	if !trepeatIp {
		total_count.bfIpCount++
	}

	//tmp_count.package_count[vec[6]]++
	//total_count.package_count[vec[6]]++
	if !repeatPack {
		tmp_count.bfPackCount++
	}
	if !trepeatPack {
		total_count.bfPackCount++
	}

	v.m_record_lock.Unlock() //unlock
}

// Write to file
func (v *logset) Out() {
	for {
		time.Sleep(2 * time.Second)
		v.m_record_lock.RLock()
		v.outImmediately()
		v.m_record_lock.RUnlock()
		v.Release()
	}
}

func (v *logset) Release() {
	releasename := "./stat/cachingpackagesignquery/" + strings.Replace(v.date, "-", "", -1) + "/caching_package_sign_query_stat_log.log"

	os.Rename("cachingpackagesignquery"+strings.Replace(v.date, "/", "-", -1)+".done", releasename)

	cmd := exec.Command("gzip", "-S .gz.done", releasename)
	err := cmd.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, time.Now(), "gzip : ", err)
	}
}

//
func (v *logset) outImmediately() {
	//v.cacheinfile()

	tmplog, err := logger.NewDataLogger("./", "cachingpackagesignquery"+strings.Replace(v.date, "/", "-", -1))
	if err != nil {
		fmt.Fprintln(os.Stderr, time.Now(), "An error occurred on opening file : ", err)
		panic(err)
	}
	defer tmplog.Finish()

	// dump data to file
	tmplog.Writeln("connect_count\tquery_count\tmatch_count\tuser_count\tip_count\tpackage_count\tdate\tversion")

	total_count := v.allversion_count
	tmplog.Writeln(total_count.connect_counts, "\t",
		total_count.query_counts, "\t",
		total_count.match_count, "\t",
		//len(total_count.user_counts), "\t",
		total_count.bfUserCounts, "\t",
		//len(total_count.ip_count), "\t",
		total_count.bfIpCount, "\t",
		//len(total_count.package_count), "\t",
		total_count.bfPackCount, "\t",
		v.date, "\t",
		1000000000)

	for version, count := range v.version_count {
		tmplog.Writeln(count.connect_counts, "\t",
			count.query_counts, "\t",
			count.match_count, "\t",
			//len(count.user_counts), "\t",
			count.bfUserCounts, "\t",
			//len(count.ip_count), "\t",
			count.bfIpCount, "\t",
			//len(count.package_count), "\t",
			count.bfPackCount, "\t",
			v.date, "\t",
			version)
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
		} else if sign == "match_count" {
			totalCount.match_count += i64
			tmp_count.match_count = i64
		}
	case 4:
		if sign == "uuid" {
			totalCount.user_counts[vec[2]] += i64
			tmp_count.user_counts[vec[2]] = i64
		} else if sign == "ip" {
			totalCount.ip_count[vec[2]] += i64
			tmp_count.ip_count[vec[2]] = i64
		} else if sign == "package" {
			totalCount.package_count[vec[2]] += i64
			tmp_count.package_count[vec[2]] = i64
		}
	default:
		fmt.Fprintln(os.Stderr, time.Now(), "wrong data")
	}
}

func (v *logset) readcachefile() {
	midlog, err := logger.NewCacheLog("./", "cachingpackagesignquerymiddle"+strings.Replace(v.date, "/", "-", -1)+".done")
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

func (v *logset) cacheinfile() {
	midlog, err := logger.NewDataLogger("./", "cachingpackagesignquerymiddle"+strings.Replace(v.date, "/", "-", -1))
	if err != nil {
		fmt.Fprintln(os.Stderr, time.Now(), "An error occurred on opening file : ", err)
		panic(err)
	}
	defer midlog.Finish()

	for version, count := range v.version_count {
		midlog.Writeln(version, "\tconnect_counts\t", count.connect_counts)
		midlog.Writeln(version, "\tquery_counts\t", count.query_counts)
		midlog.Writeln(version, "\tmatch_count\t", count.match_count)

		for uuid, num := range count.user_counts {
			midlog.Writeln(version, "\tuuid\t", uuid+"\t", num)
		}

		for remoteIP, num := range count.ip_count {
			midlog.Writeln(version, "\tip\t", remoteIP+"\t", num)
		}

		for pack, num := range count.package_count {
			midlog.Writeln(version, "\tpackage\t", pack+"\t", num)
		}
	}
}
