package mplogger

import (
	"fmt"
	"log"
	"runtime"
)

func stackInfo(depth int) (string, int) {
	pc, _, line, ok := runtime.Caller(depth + 1)
	if !ok {
		panic("look at what goes wrong?")
	}
	fn := runtime.FuncForPC(pc)

	return fn.Name(), line
}

// controls debug printing
// var debugOptions = []bool{true, false, false, false, false, false, false, false}

const InfoColor = "%s"
const ErrorColor = "\033[1;31m%s\033[0m"            // red
const RoleChangeColor = "\033[1;48;5;198m%s\033[0m" // DeepPink1 backgrpund
const SendColor1 = "\033[1;48;5;65m%s\033[0m"       // DarkSeaGreen4 background
const SendColor2 = "\033[1;48;5;179m%s\033[0m"      // LightGoldenrod3 background
const SendColor3 = "\033[1;48;5;246m%s\033[0m"      // Grey58 background
const CommitColor = "\033[1;41m%s\033[0m"
const GatherVoteColor = "\033[1;32m%s\033[0m"                         // green
const EntryColor = "\033[1;34m%s\033[0m"                              // blue
const SnapshotColor = "\033[1;38;5;100m%s\033[0m"                     // Yellow4
const PersistColor = "\033[1;48;5;100m%s\033[0m"                      // Yellow4
const QuorumColor = "\033[1;48;5;65m\033[1;38;5;217m%s\033[0m\033[0m" //DarkSeaGreen4 with LightPink1

const SpecialColor1 = "\033[4;38;5;204m%s\033[0m" // 256 bit color, pink with underline

type debugOption struct {
	prefix     string
	stackDepth int
	enable     bool
	color      string
}

const debugOn = false

var dos map[int]debugOption = map[int]debugOption{
	0: {prefix: "ERROR", stackDepth: 1, enable: false, color: ErrorColor},
	1: {prefix: "INFO", stackDepth: 1, enable: false, color: InfoColor},
	2: {prefix: "ROLE-CHANGE", stackDepth: 1, enable: false, color: RoleChangeColor},
	3: {prefix: "VOTE", stackDepth: 1, enable: false, color: GatherVoteColor},
	4: {prefix: "SEND-ELECTION", stackDepth: 1, enable: false, color: SendColor1},
	5: {prefix: "SEND-APPEND", stackDepth: 2, enable: false, color: SendColor2},
	6: {prefix: "SEND-REPLY", stackDepth: 2, enable: false, color: SendColor3},
	7: {prefix: "COMMIT", stackDepth: 1, enable: false, color: CommitColor},
	8: {prefix: "DEEP-INFO", stackDepth: 2, enable: false, color: InfoColor},
}

type RaftLogger struct {
	log  log.Logger
	role string
	term uint64
	id   int
}

func (p *RaftLogger) Error(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[0], format, args...)
}

func (p *RaftLogger) Info(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[1], format, args...)
}

func (p *RaftLogger) InfoRoleChange(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[2], format, args...)
}

func (p *RaftLogger) DebugVote(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[3], format, args...)
}

func (p *RaftLogger) DebugSendElection(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[4], format, args...)
}

func (p *RaftLogger) DebugSendAppend(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[5], format, args...)
}

func (p *RaftLogger) DebugReply(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[6], format, args...)
}

func (p *RaftLogger) InfoCommit(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[7], format, args...)
}

func (p *RaftLogger) InfoDeep(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[8], format, args...)
}

func (p *RaftLogger) debugPrintWrapper(debug debugOption, format string, args ...interface{}) {
	if !debug.enable {
		return
	}
	lines := []int{}
	for i := 1; i <= debug.stackDepth; i++ {
		_, line := stackInfo(i + 1)
		lines = append(lines, line)
	}
	common := p.commonPrint()
	str := fmt.Sprintf("[%s(%v)| %s] %s", debug.prefix, lines, common, format)
	str = fmt.Sprintf(debug.color, str)
	p.log.Printf(str, args...)
}

func (p *RaftLogger) commonPrint() string {

	str := fmt.Sprintf("replica %d term %d", p.id, p.term)
	return str
}

func NewRaftDebugLogger() *RaftLogger {
	p := &RaftLogger{}
	p.log = *log.Default()
	p.log.SetFlags(log.Ltime | log.Lmicroseconds)

	return p
}

func (p *RaftLogger) SetContext(role string, term uint64, id int) {
	p.id = id
	p.role = role
	p.term = term
}
