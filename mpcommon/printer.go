package mpcommon

import (
	"fmt"
	"log"
	"runtime"
)

const InfoColor = "%s"
const ErrorColor = "\033[1;31m%s\033[0m"      // red
const TodoColor = "\033[1;48;5;198m%s\033[0m" // DeepPink1 backgrpund

type debugOption struct {
	prefix     string
	stackDepth int
	enable     bool
	color      string
}

const debugOn = false

var dos map[int]debugOption = map[int]debugOption{
	0: {prefix: "ERROR", stackDepth: 1, enable: debugOn, color: ErrorColor},
	1: {prefix: "INFO", stackDepth: 1, enable: debugOn, color: InfoColor},
	2: {prefix: "INFO2", stackDepth: 2, enable: false, color: InfoColor},
	3: {prefix: "TODO", stackDepth: 1, enable: debugOn, color: TodoColor},
}

type Printer struct {
	printer     log.Logger
	CommonPrint func() string
}

func (p *Printer) Error(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[0], format, args...)
}

func (p *Printer) Info(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[1], format, args...)
}

func (p *Printer) Info2(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[2], format, args...)
}

func (p *Printer) Todo(format string, args ...interface{}) {
	p.debugPrintWrapper(dos[3], format, args...)
}

func stackInfo(depth int) (string, int) {
	pc, _, line, ok := runtime.Caller(depth + 1)
	if !ok {
		panic("look at what goes wrong?")
	}
	fn := runtime.FuncForPC(pc)

	return fn.Name(), line
}

func (p *Printer) debugPrintWrapper(debug debugOption, format string, args ...interface{}) {
	if !debug.enable {
		return
	}
	_, line := stackInfo(debug.stackDepth + 1)
	common := p.CommonPrint()
	str := fmt.Sprintf("[%s(%5d)| %s] %s", debug.prefix, line, common, format)
	str = fmt.Sprintf(debug.color, str)
	p.printer.Printf(str, args...)
}
