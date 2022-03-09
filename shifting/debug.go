package main

import (
	"fmt"
	"log"
	"os"
)

type Logger struct {
	logger   *log.Logger
	colorFmt string
	prefix   string
}

func NewLogger(prefix string, colorScheme string) *Logger {
	l := &Logger{}
	l.logger = log.New(os.Stdout, "", log.Lmicroseconds)
	l.colorFmt = colorScheme
	if colorScheme == "" {
		l.colorFmt = "%s"
	}
	l.prefix = prefix

	return l
}

func (l *Logger) Info(format string, args ...interface{}) {
	printThing := fmt.Sprintf(format, args...)
	printThing = fmt.Sprintf("%s %s", l.prefix, printThing)
	l.logger.Printf(l.colorFmt, printThing)
}

func (l *Logger) Timeout(format string, args ...interface{}) {
	printThing := fmt.Sprintf(format, args...)
	printThing = fmt.Sprintf("%s %s", l.prefix, printThing)

	colorFmt := "\033[1;31m%s\033[0m"
	colorFmt = fmt.Sprintf(l.colorFmt, colorFmt)
	l.logger.Printf(colorFmt, printThing)
}

func (l *Logger) Vote(format string, args ...interface{}) {
	printThing := fmt.Sprintf(format, args...)
	printThing = fmt.Sprintf("%s %s", l.prefix, printThing)

	colorFmt := "\033[1;32m%s\033[0m"
	colorFmt = fmt.Sprintf(l.colorFmt, colorFmt)
	l.logger.Printf(colorFmt, printThing)
}

func (l *Logger) GatherVote(format string, args ...interface{}) {
	printThing := fmt.Sprintf(format, args...)
	printThing = fmt.Sprintf("%s %s", l.prefix, printThing)

	colorFmt := "\033[1;94m%s\033[0m"
	colorFmt = fmt.Sprintf(l.colorFmt, colorFmt)
	l.logger.Printf(colorFmt, printThing)
}
