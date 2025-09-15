package jira

/*
Package jira implements JIRA API integration and processing functionality
debug logs a debug level message if the logger is initialized
format: the format string for the message
args: optional arguments for format string interpolation
*/
func (j *jiraProc) debug(format string, args ...interface{}) {
	if j != nil && j.log != nil {
		j.log.Debugf(format, args...)
	}
}

/*
info logs an info level message if the logger is initialized
format: the format string for the message
args: optional arguments for format string interpolation
*/
func (j *jiraProc) info(format string, args ...interface{}) {
	if j != nil && j.log != nil {
		j.log.Infof(format, args...)
	}
}

/*
warn logs a warning level message if the logger is initialized
format: the format string for the message
args: optional arguments for format string interpolation
*/
func (j *jiraProc) warn(format string, args ...interface{}) {
	if j != nil && j.log != nil {
		j.log.Warnf(format, args...)
	}
}

/*
error logs an error level message if the logger is initialized
format: the format string for the message
args: optional arguments for format string interpolation
*/
func (j *jiraProc) error(format string, args ...interface{}) {
	if j != nil && j.log != nil {
		j.log.Errorf(format, args...)
	}
}
